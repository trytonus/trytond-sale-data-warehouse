# -*- coding: utf-8 -*-
"""
    sale.py

"""
import logging

from sql.functions import ToChar
from sql.operators import Mul
from trytond.pool import PoolMeta, Pool
from trytond.transaction import Transaction

try:
    import psycopg2
except ImportError:
    psycopg2 = None


__all__ = ['SaleLine']
__metaclass__ = PoolMeta


class SaleLine:
    __name__ = 'sale.line'

    @classmethod
    def __register__(cls, module_name):
        super(SaleLine, cls).__register__(module_name)
        if not Pool.test:
            cls.build_data_warehouse()

    @classmethod
    def get_warehouse_query(cls):
        """
        Returns the data warehouse select query. This query only returns a
        select query and the tables themselves, but does not execute it. This
        gives downstream modules room to change the query.
        """
        def table(pool_name): return Pool().get(pool_name).__table__()

        sale_line = table('sale.line')
        sale_sale = table('sale.sale')

        product_template = table('product.template')
        product_product = table('product.product')
        product_category = table('product.category')

        party_party = table('party.party')
        shipment_address = table('party.address')
        invoice_address = table('party.address')

        invoice_country = table('country.country')
        invoice_subdivision = table('country.subdivision')
        shipment_country = table('country.country')
        currency_currency = table('currency.currency')
        shipment_subdivision = table('country.subdivision')

        tables = {
            'sale.line': sale_line,
            'sale.sale': sale_sale,
            'product.template': product_template,
            'product.product': product_product,
            'product.category': product_category,
            'party.party': party_party,
        }

        columns = [
            sale_line.id.as_('id'),
            sale_line.quantity.as_('quantity'),
            Mul(sale_line.quantity, sale_line.unit_price).as_('amount'),

            product_product.code.as_('product_code'),
            product_template.name.as_('product_name'),
            product_category.name.as_('product_category'),

            party_party.name.as_('party_name'),
            party_party.id.as_('party_id'),

            # Sale primary data
            sale_sale.id.as_('sale_id'),
            sale_sale.reference.as_('sale_reference'),
            currency_currency.code.as_('currency'),
            sale_sale.state.as_('state'),

            # Address, country information
            invoice_country.code.as_('invoice_country_code'),
            invoice_country.name.as_('invoice_country_name'),
            invoice_subdivision.code.as_('invoice_state_code'),
            invoice_subdivision.name.as_('invoice_state_name'),
            shipment_country.code.as_('shipment_country_code'),
            shipment_country.name.as_('shipment_country_name'),
            shipment_subdivision.code.as_('shipment_state_code'),
            shipment_subdivision.name.as_('shipment_state_name'),

            # Sale date and subparts
            sale_sale.sale_date.as_('sale_date'),
            ToChar(sale_sale.sale_date, 'YYYY').as_('sale_year'),
            ToChar(sale_sale.sale_date, 'MM').as_('sale_month'),
            ToChar(sale_sale.sale_date, 'dd').as_('sale_day'),
        ]
        from_ = sale_line.join(
            sale_sale,
            condition=(sale_line.sale == sale_sale.id)
        ).join(
            product_product, 'LEFT OUTER',
            (sale_line.product == product_product.id)
        ).join(
            product_template, 'LEFT OUTER',
            (product_product.template == product_template.id)
        ).join(
            product_category, 'LEFT OUTER',
            (product_template.category == product_category.id)
        ).join(
            party_party, 'LEFT OUTER',
            (sale_sale.party == party_party.id)
        ).join(
            shipment_address, 'LEFT OUTER',
            (sale_sale.shipment_address == shipment_address.id)
        ).join(
            shipment_country, 'LEFT OUTER',
            (shipment_address.country == shipment_country.id)
        ).join(
            shipment_subdivision, 'LEFT OUTER',
            (shipment_address.subdivision == shipment_subdivision.id)
        ).join(
            invoice_address, 'LEFT OUTER',
            (sale_sale.invoice_address == invoice_address.id)
        ).join(
            invoice_country, 'LEFT OUTER',
            (invoice_address.country == invoice_country.id)
        ).join(
            invoice_subdivision, 'LEFT OUTER',
            (invoice_address.subdivision == invoice_subdivision.id)
        ).join(
            currency_currency, 'LEFT OUTER',
            (sale_sale.currency == currency_currency.id)
        )

        try:
            sale_channel = table('sale.channel')
        except KeyError:
            # Module is not installed
            pass
        else:
            from_ = from_.join(
                sale_channel, 'LEFT OUTER',
                (sale_sale.channel == sale_channel.id)
            )
            columns.extend([
                sale_channel.code.as_('channel_code'),
                sale_channel.name.as_('channel_name'),
            ])

        # Build a where clause for states
        where = (
            sale_sale.state.in_(('confirmed', 'processing', 'done'))
        ) & (
            sale_line.type == 'line'
        )

        return from_, columns, where, tables

    @classmethod
    def build_data_warehouse(cls):
        """
        Build the data warehouse. Depending on the backend that you
        are using your downstream module might want to overwrite this.
        This creates a new materialized view without data.
        """
        from_, columns, where, _ = cls.get_warehouse_query()
        rebuild_query = from_.select(where=where, *columns)
        cursor = Transaction().connection.cursor()

        cursor(
            "DROP MATERIALIZED VIEW IF EXISTS dw_sale_line"
        )
        cursor(
            "CREATE MATERIALIZED VIEW dw_sale_line AS " + str(rebuild_query) +
            " WITH NO DATA", rebuild_query.params
        )
        # Index is required to refresh materialized view CONCURRENTLY
        cursor(
            "CREATE UNIQUE INDEX unique_id ON dw_sale_line (id)"
        )

    @classmethod
    def refresh_data_warehouse(cls):
        """
        Refresh data in data warehouse and commit here itself
        """
        logger = logging.getLogger('SALE_LINE_DATA_WAREHOUSE')

        if not psycopg2:
            logger.info('psycopg2 not found')
            return
        try:
            with Transaction().new_transaction() as transaction:
                cursor = transaction.connection.cursor()
                cursor.execute(
                    "REFRESH MATERIALIZED VIEW CONCURRENTLY dw_sale_line"
                )
                transaction.commit()
        except (psycopg2.NotSupportedError, psycopg2.ProgrammingError), e:
            if 'CONCURRENTLY' not in e.message:
                # Raise is error is not because of 'CONCURRENTLY'
                raise
            logger.info(
                'CONCURRENTLY Materialized refresh failed, proceeding to '
                'Normal refresh'
            )
            Transaction().rollback()
            # Refresh view normally
            with Transaction().new_transaction() as transaction:
                cursor = transaction.connection.cursor()
                cursor.execute(
                    "REFRESH MATERIALIZED VIEW dw_sale_line"
                )
                transaction.commit()

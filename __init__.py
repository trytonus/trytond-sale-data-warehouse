# -*- coding: utf-8 -*-
"""
    __init__.py

"""
from trytond.pool import Pool
from sale import SaleLine


def register():
    Pool.register(
        SaleLine,
        module='sale_data_warehouse', type_='model'
    )

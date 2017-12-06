from __future__ import division, print_function, absolute_import

import MySQLdb
import sys
import pymongo
import petl as etl
import json
import logging
import logging.handlers
import os
import urllib


from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict
from datetime import date, datetime

from pattern.text.en import singularize

from utilities import utils


# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

# sanitize categories, need a better way to do this, perhaps a stemming lib
PLURAL_CATEGORIES = ['Seeds', 'Drinks', 'Edibles']

logging.basicConfig(filename="logs/g1-etl-menuitems.log", level=logging.INFO)
log = logging.getLogger("g1-etl-menuitems")


def extract(organization_id):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-10171"
                                "7-cluster.cluster-cmtxwpwvylo7.us-west-2.rds"
                                ".amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")

    try:
        mmj_menu_items = load_db_data(source_db, 'menu_items')
        mmj_categories = load_db_data(source_db, 'categories')
        prices = load_db_data(source_db, 'menu_item_prices')

        return transform(mmj_menu_items, mmj_categories,
                         prices, organization_id)

    finally:
        source_db.close()


def transform(mmj_menu_items, mmj_categories, prices, organization_id):
    """
    Transform data
    """
    # source data table
    source_dt = view_to_list(mmj_menu_items)
    cut_menu_data = ['id', 'vendor_id', 'menu_id', 'dispensary_id',
                     'strain_id', 'created_at', 'updated_at', 'category_id',
                     'name', 'sativa', 'indica', 'on_hold']

    cut_prices = ['menu_item_id', 'price_half_gram', 'price_gram',
                  'price_two_gram', 'price_eigth', 'price_quarter',
                  'price_half', 'price_ounce']
    # Cut out all the fields we don't need to load
    menu_items = etl.cut(source_dt, cut_menu_data)
    prices_data = etl.cut(prices, cut_prices)

    menu_items = (
        etl
        .addfield(menu_items, 'organizationId')
        .addfield('createdAtEpoch')
        .addfield('keys')
    )

    # Two-step transform and cut. First we need to cut the name
    # and id from the source data to map to.
    cut_source_cats = etl.cut(mmj_categories, 'name', 'id')
    source_values = etl.values(cut_source_cats, 'name', 'id')

    # Then we nede a dict of categories to compare against.
    # id is stored to match against when transforming and mapping categories
    mmj_categories = dict([(value, id) for (value, id) in source_values])

    mappings = OrderedDict()
    mappings['id'] = 'id'
    mappings['createdAt'] = 'created_at'
    mappings['updatedAt'] = 'updated_at'
    mappings['name'] = 'name'
    mappings['organizationId'] = organization_id
    mappings['categoryId'] = \
        lambda x: map_categories(x.category_id, mmj_categories, menu_items)
    mappings['active'] = lambda x: True if x.on_hold == 1 else False

    fields = etl.fieldmap(menu_items, mappings)
    data = etl.merge(menu_items, fields, key='id')

    items = []
    for item in etl.dicts(data):
        breakpoint_pricing = (
            etl
            .select(prices_data, lambda x: x.menu_item_id == item['menu_id'])
            .rename({'price_eigth': 'price_eighth'})
            .cutout('menu_item_id')
        )

        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id'],
            'menu_id': item['menu_id'],
            'vendor_id': item['vendor_id'],
            'strain_id': item['strain_id'],
            'category_id': item['category_id']
        }
        for price in etl.dicts(breakpoint_pricing):
            item['weightPricing'] = {
               'price_half_gram': price['price_half_gram'],
               'price_gram': price['price_gram'],
               'price_eighth': price['price_eighth'],
               'price_quarter': price['price_quarter'],
               'price_half': price['price_half'],
               'price_ounce': price['price_ounce']
            }

        del item['vendor_id']
        del item['indica']
        del item['dispensary_id']
        del item['id']
        del item['strain_id']
        del item['on_hold']
        del item['menu_id']
        del item['sativa']
        del item['category_id']
        del item['updated_at']
        del item['created_at']
        # set up final structure for API
        items.append(item)

    result = json.dumps(items, sort_keys=True, indent=4, default=json_serial)
    print(result)
    return result


def map_categories(category_id, data, menu_items):
    try:
        category = data.keys()[data.values().index(category_id)]
        if category == 'Cannabis':
            strain_data = etl.cut(menu_items, 'sativa', 'indica',
                                              'category_id', 'id')
            strain_vals = etl.dicts(strain_data)
            for strain in strain_vals.__iter__():
                if strain['sativa'] > 0 or strain['indica'] > 0:
                    if strain['sativa'] >= 80:
                        return 'Sativa'
                    if strain['indica'] >= 80:
                        return 'Indica'
                else:
                    return 'Hybrid'
        if category == 'Paraphernalia':
            return 'Gear'
        if category == 'Tincture':
            return 'Tinctures'
        if category == 'Prerolled':
            return 'Preroll'

        if category in PLURAL_CATEGORIES:
            return singularize(category)
        else:
            return category
    except ValueError:
        return 'Other'


def lab_results(data):
    # Put lab results on their own as this will be its own collection later
    lab_results = etl.cut(data, *range(11, 16))
    return lab_results


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def load_db_data(db, table_name):
    """
    Data extracted from source db
    """
    return etl.fromdb(db, "SELECT * from {0} LIMIT 10".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(sys.argv[1])

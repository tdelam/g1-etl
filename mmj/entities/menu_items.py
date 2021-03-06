from __future__ import division, print_function, absolute_import

import os
import sys
import inspect
import petl as etl
import json

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from copy import copy
from collections import OrderedDict
from pattern.text.en import singularize

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe()))
)
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from utilities import utils

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding("latin-1")

# sanitize categories, need a better way to do this, perhaps a stemming lib
PLURAL_CATEGORIES = ['Seeds', 'Drinks', 'Edibles']
CAT_MAP = ['Indica', 'Sativa', 'Hybrid', 'Edible', 
           'Concentrate', 'Drink', 'Clone', 'Seed', 
           'Tinctures', 'Gear', 'Topicals', 'Preroll',
           'Wax', 'Hash']

def extract(dispensary_id, organization_id, debug):
    """
    Grab all data from source(s).
    """
    source_db = utils.mysql_connect()
    try:
        mmj_menu_items = utils.load_db_data(source_db, dispensary_id, 'menu_items')
        mmj_categories = utils.load_db_data(source_db, dispensary_id, 'categories')
        prices = utils.load_db_data(source_db, dispensary_id, 'menu_item_prices')

        return transform(mmj_menu_items, mmj_categories, prices,
                         organization_id, source_db, debug)

    finally:
        source_db.close()


def transform(mmj_menu_items, mmj_categories, prices,
              organization_id, source_db, debug):
    """
    Transform data
    """
    # source data table
    source_dt = utils.view_to_list(mmj_menu_items)

    cut_menu_data = ['id', 'vendor_id', 'menu_id', 'dispensary_id',
                     'strain_id', 'created_at', 'updated_at', 'category_id',
                     'name', 'sativa', 'indica', 'on_hold', 'product_type',
                     'image_file_name', 'medicine_amount', 'product_type']

    cut_prices = ['menu_item_id', 'dispensary_id', 'price_half_gram', 'price_gram',
                  'price_two_gram', 'price_eigth', 'price_quarter',
                  'price_half', 'price_ounce']

    # Cut out all the fields we don't need to load
    menu_items = etl.cut(source_dt, cut_menu_data)
    prices_data = etl.cut(prices, cut_prices)

    menu_items = (
        etl
        .addfield(menu_items, 'createdAtEpoch')
        .addfield('unitOfMeasure')
        .addfield('locationProductDetails')
        .addfield('keys')
        .addfield('restockLevel')
    )

    # Two-step transform and cut. First we need to cut the name
    # and id from the source data to map to.
    cut_source_cats = etl.cut(mmj_categories, 'name', 'id', 'measurement')
    source_values = etl.values(cut_source_cats, 'name', 'id')

    # Then we nede a dict of categories to compare against.
    # id is stored to match against when transforming and mapping categories
    mmj_categories = dict([(value, id) for (value, id) in source_values])

    mappings = OrderedDict()
    mappings['id'] = 'id'
    mappings['createdAt'] = 'created_at'
    mappings['updatedAt'] = 'updated_at'
    mappings['createdAtEpoch'] = lambda x: utils.create_epoch(x.created_at)
    mappings['name'] = 'name'
    mappings['shareOnWM'] = lambda x: _wm_integration(x.id, source_db)
    """
    1 = Units
    2 = Grams (weight)
    """
    mappings['unitOfMeasure'] = \
        lambda x: _map_uom(x.category_id, source_db)

    fields = etl.fieldmap(menu_items, mappings)
    data = etl.merge(menu_items, fields, key='id')

    items = []
    for item in etl.dicts(data):
        
        breakpoint_pricing = (
            etl
            .select(prices_data, lambda x: x.dispensary_id == item['dispensary_id'])
            .rename({'price_eigth': 'price_eighth'})
            .cutout('menu_item_id')
        )
        # Set image url for load to download
        url = None
        if debug and item['image_file_name'] is not None:
            url = ("https://wm-mmjmenu-images-development.s3."
                   "amazonaws.com/menu_items/images/{0}/large/"
                   "{1}").format(item['id'], item['image_file_name'])
        elif item['image_file_name'] is not None:
            url = ("https://wm-mmjmenu-images-production.s3."
                   "amazonaws.com/menu_items/images/{0}/large/"
                   "{1}").format(item['id'], item['image_file_name'])

        item['image_file_name'] = url

        item['categoryId'] = _map_categories(item['category_id'],
                                            item['sativa'], item['indica'],
                                            mmj_categories, menu_items)
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id'],
            'menu_id': item['menu_id'],
            'vendor_id': item['vendor_id'],
            'strain_id': item['strain_id'],
            'category_id': item['category_id']
        }
        
        # set a default netMJ value if the menu item is a unit product
        if item['unitOfMeasure'] is 2:
            item['netMarijuana'] = int(item['medicine_amount'])

        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

        item['locationProductDetails'] = {
            'id': item['id'],
            'active': _active(item['on_hold'])
        }

        item['restockLevel'] = _restock_level(item['dispensary_id'],
                                              item['product_type'], source_db)

        if item['shareOnWM'] is None:
            item['shareOnWM'] = False

        for price in etl.dicts(breakpoint_pricing):
            try:
                price_two_gram = price['price_two_gram']
            except KeyError:
                price_two_gram = 0.0

            item['locationProductDetails']['weightPricing'] = {
               'price_half_gram': utils.dollars_to_cents(price['price_half_gram']),
               'price_two_gram': utils.dollars_to_cents(price_two_gram),
               'price_gram': utils.dollars_to_cents(price['price_gram']),
               'price_eighth': utils.dollars_to_cents(price['price_eighth']),
               'price_quarter': utils.dollars_to_cents(price['price_quarter']),
               'price_half': utils.dollars_to_cents(price['price_half']),
               'price_ounce': utils.dollars_to_cents(price['price_ounce'])
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
        del item['product_type']

        if item['image_file_name'] is None:
            del item['image_file_name']
            
        # set up final structure for API
        items.append(item)

    # Remove inactive items
    for item in items:
        if item['locationProductDetails']['active'] is False:
            items.remove(item)

    if debug:
        result = json.dumps(items, sort_keys=True,
                            indent=4, default=utils.json_serial)
        print(result)


    return items


def _active(on_hold):
    """
    Sets the active bit on items
    """
    if on_hold == 1:
        return True
    return False


def _restock_level(id, product_type, source_db):
    """
    Since G1 does not have a global setting for low inventory settings,
    we will need to populate all products with the MMJ inventory settings.
    """
    sql = ("SELECT dispensary_id, grams_hold_at, units_hold_at "
           "FROM dispensary_details "
           "WHERE dispensary_id={0}").format(id)
    data = etl.fromdb(source_db, sql)

    restock = []
    if product_type == 1:
        level = etl.lookup(data, 'dispensary_id', 'grams_hold_at')
    else:
        level = etl.lookup(data, 'dispensary_id', 'units_hold_at')
    return level[id][0]


def _wm_integration(id, source_db):
    """
    If menu_item_id exists in menu_item_weedmaps_integrations then 
    shareOnWm is true.
    """
    sql = ("SELECT DISTINCT menu_item_id id "
           "FROM menu_item_weedmaps_integrations "
           "WHERE menu_item_id={0}").format(id)

    data = etl.fromdb(source_db, sql) 
    exists = etl.lookup(data, 'id')

    if exists[id][0] is not None:
        return True
    return False


def _map_uom(category_id, source_db):
    """
    Maps the UOM.
        This is going to look backwards but it's because on G1 the enum for
        UOM is:
            GRAM: 1
            EACH: 2
        but MMJ uses:
            UNITS: 1
            GRAM: 2
    """

    sql = ("SELECT DISTINCT measurement, id "
           "FROM categories "
           "WHERE id={0}").format(category_id)

    data = etl.fromdb(source_db, sql) 
    measurement = etl.lookup(data, 'id', 'measurement')
    if measurement[category_id][0] == 1:
        return 2
    return 1


def _map_categories(category_id, sativa, indica, data, menu_items):
    """
    If the menu item that are % indica and % sativa. If > indica threshold,
    it goes into indica, if > sativa threshold it goes into sativa,
    if neither it goes into hybrid. The other conditions within this will
    map to G1's naming convention, i.e: MMJ Drinks => G1 Drink
    """
    category = data.keys()[data.values().index(category_id)]
    if category.lower() == 'cannabis':
        if sativa > 0 and indica > 0:
            if sativa > 80:
                return 'Sativa'
            if indica > 80:
                return 'Indica'
        else:
            return 'Hybrid'

    if category.lower() == 'paraphernalia':
        return 'Gear'
    if category.lower() == 'tincture':
        return 'Tinctures'
    if category.lower() == 'prerolled':
        return 'Preroll'
    if category in PLURAL_CATEGORIES:
        return singularize(category)
    if category not in CAT_MAP:
        return 'Other'
    return category


def lab_results(data):
    # Put lab results on their own as this will be its own collection later
    lab_results = etl.cut(data, *range(11, 16))
    return lab_results


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2], True)

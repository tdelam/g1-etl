from __future__ import division, print_function, absolute_import

import os,sys,inspect
import MySQLdb
import petl as etl
import json

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

from utilities import utils
from collections import OrderedDict
from pattern.text.en import singularize

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

# sanitize categories, need a better way to do this, perhaps a stemming lib
PLURAL_CATEGORIES = ['Seeds', 'Drinks', 'Edibles']


def extract(organization_id, debug):
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
        mmj_menu_items = utils.load_db_data(source_db, 'menu_items')
        mmj_categories = utils.load_db_data(source_db, 'categories')
        prices = utils.load_db_data(source_db, 'menu_item_prices')

        return transform(mmj_menu_items, mmj_categories,
                         prices, organization_id, debug)

    finally:
        source_db.close()


def transform(mmj_menu_items, mmj_categories, prices, organization_id, debug):
    """
    Transform data
    """
    # source data table
    source_dt = utils.view_to_list(mmj_menu_items)
    cut_menu_data = ['id', 'vendor_id', 'menu_id', 'dispensary_id',
                     'strain_id', 'created_at', 'updated_at', 'category_id',
                     'name', 'sativa', 'indica', 'on_hold']

    cut_prices = ['menu_item_id', 'price_half_gram', 'price_gram',
                  'price_two_gram', 'price_eigth', 'price_quarter',
                  'price_half', 'price_ounce']

    cut_wm = ['menu_item_id', 'weedmaps_integration_id', 'weedmaps_id']
    # Cut out all the fields we don't need to load
    menu_items = etl.cut(source_dt, cut_menu_data)
    prices_data = etl.cut(prices, cut_prices)

    menu_items = (
        etl
        .addfield(menu_items, 'organizationId')
        .addfield('createdAtEpoch')
        .addfield('unitOfMeasure')
        .addfield('keys')
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
    mappings['name'] = 'name'

    """
    1 = Units
    2 = Grams (weight)
    """
    mappings['unitOfMeasure'] = \
        lambda x: map_uom(x.category_id, cut_source_cats)

    mappings['organizationId'] = organization_id

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

        item['categoryId'] = map_categories(item['category_id'],
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

    if debug:
        result = json.dumps(items, sort_keys=True,
                            indent=4, default=utils.json_serial)
        print(result)

    return items


def map_uom(category_id, categories):
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
    measurement = etl.selecteq(categories, 'id', category_id)
    if etl.nrows(measurement) > 0:
        for measure in etl.dicts(measurement):
            return 2 if measure['measurement'] == 1 else 1


def map_categories(category_id, sativa, indica, data, menu_items):
    """
    If the menu item that are % indica and % sativa. If > indica threshold, 
    it goes into indica, if > sativa threshold it goes into sativa, 
    if neither it goes into hybrid. The other conditions within this will
    map to G1's naming convention, i.e: MMJ Drinks => G1 Drink
    """
    try:
        category = data.keys()[data.values().index(category_id)]
        if category == 'Cannabis':
            if sativa > 0 and indica > 0:
                if sativa > 80:
                    return 'Sativa'
                if indica > 80:
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


if __name__ == '__main__':
    extract(sys.argv[1], True)

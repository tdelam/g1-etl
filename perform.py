from __future__ import division, print_function, absolute_import

import petl as etl
import MySQLdb
import sys
import itertools
import pymongo
import re

from petl.io.db import DbView
from petl.io.json import DictsView

from pymongo import MongoClient
from sqlalchemy import *

from pattern.text.en import singularize

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')


def extract(table_name, collection):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    target_db = pymongo.MongoClient("mongodb://127.0.0.1:3001")

    try:
        source_data = load_db_data(source_db, table_name)
        source_ctx = load_db_data(source_db, 'categories')

        target_data = load_mongo_data(target_db, '{0}'.format(collection))
        target_ctx = load_mongo_data(target_db, 'inventory.categories')

        transform_menu_items(source_data, target_data, source_ctx, target_ctx)

    finally:
        source_db.close()
        target_db.close()


def transform_menu_items(source_data, target_data, source_ctx, target_ctx):
    """
    Load the transformed data into the destination(s)
    +-------------+--------------+------+------+------+
    | thc_percent | thca_percent | cbn  | cbd  | cbda |
    +=============+==============+======+======+======+
    |         0.0 |          0.0 |  0.0 |  0.0 |  0.0 |
    +-------------+--------------+------+------+------+
    |         0.0 |          0.0 |  0.0 |  0.0 |  0.0 |
    +-------------+--------------+------+------+------+
    """
    # source data table
    source_dt = dbview_to_list(source_data)

    # Cut out all the fields we don't need to load
    menu_items = etl.cutout(source_dt, 'menu_id', 'body_html',
                            'deduct_from_id', 'tested_by', 'medicine_amount',
                            'medicine_measurement', 'batch_number', 'barcode',
                            'custom_barcode', 'taxable', 'image_updated_at',
                            'consignment', 'mmjrevu_id', 'sclabs_report_id',
                            'override_global_dollars_to_points',
                            'dollars_to_points', 'ingredients', 'created_at',
                            'updated_at', 'deleted_at', 'lab_batch_number',
                            'lab_license_number', 'activation_time',
                            'olcc_medical_grade', 'thc_percent_min',
                            'thca_percent_min')

    # Two-step transform and cut. First we need to cut the name
    # and id from the source data to map to.
    cut_source_cats = etl.cut(source_ctx, 'name', 'id')
    source_values = etl.values(cut_source_cats, 'name', 'id')

    # Then we nede a dict of categories to compare against.
    # id is stored to match against when transforming and mapping categories
    source_ctx = dict([(value, id) for (value, id) in source_values])
    target_category_names = [name for name in target_ctx.distinct('name')]

    # TODO - Next up... create a few products on local MMJ instance with
    # category in the name of the menu item. Then, write the code to look for
    # that name against a target category name to match against.
    print(etl.look(menu_items))

    # find the menu item category id
    menu_items_cat_id = etl.values(menu_items, 'category_id')

    # sanitize categories, need a better way to do this, perhaps a stemming lib
    plural_categories = ['Seeds', 'Drinks', 'Edibles']

    # little scheme to match cats for transforming proper categories
    for item in menu_items_cat_id:
        # Separates the dictionary's values in a list, finds the position of
        # the value and gets the key at that position to match the id
        # returns source category name to compare
        source_cat_name = source_ctx.keys()[source_ctx.values().index(item)]

        if source_cat_name in plural_categories:
            source_cat_name = singularize(source_cat_name)
        # first condition, if the mmj category is found in g1
        # category, we'll assign it.
        if source_cat_name in target_category_names:
            # we found a category
            # TODO when transform - assign target data it's mongo id.
            target_cat_id = target_ctx.collection.find_one(
                {'name': source_cat_name})

    # Put lab results on their own as this will be its own collection later
    lab_results = etl.cut(menu_items, *range(11, 16))

    # Rename source fields to match target fields, rename id to mmjmenuid so
    # we can track related data
    new_header = {
        'id': 'mmjmenuid',
        'vendor_id': 'vendorId',
        'category_id': 'categoryId',
        'body': 'description'
    }

    renamed_header = etl.rename(menu_items, new_header)
    # print("this is menu items: {0}".format(etl.header(renamed_header)))
    # print("this is target data: {0}".format(etl.header(target_data)))

    # Transform!
    # Need to decide what to do with categories because UOM exists on
    # categories.measurement. If the category.measure is WEIGHT then we need
    # to look up settings and set default pricing tier. If it's UNIT we need
    # to transform to null.
    # print(etl.look(menu_items))

    # print(source_count(menu_items))
    # print(open('dump.json').read())
    # print(etl.look(lab_results))
    # etl.tojson(menu_items, 'dump.json', sort_keys=True)


def source_count(data):
    """
    Count the number of records from source(s)
    """
    if data is not None:
        return etl.nrows(data)
    return None


def destination_count(self):
    """
    Same as source_count but with destination(s)
    """
    pass


def load_mongo_data(db, collection):
    """
    Data extracted from target mongo for diff
    """
    # data from mongo needs to be treated differently.
    mongo_db = db.meteor
    return mongo_db[collection].find()


def load_db_data(db, table_name, from_json=False):
    """
    Data extracted from source db
    """
    return etl.fromdb(db, "SELECT * from {0}".format(table_name))


def dbview_to_list(data):
    if type(data) is DbView:
        # convert the db view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2])

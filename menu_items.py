from __future__ import division, print_function, absolute_import

import MySQLdb
import sys
import itertools
import pymongo
import re
import petl as etl

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict
from pymongo import MongoClient
from sqlalchemy import *

from pattern.text.en import singularize

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')

# sanitize categories, need a better way to do this, perhaps a stemming lib
PLURAL_CATEGORIES = ['Seeds', 'Drinks', 'Edibles']


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
    Transform data
    """
    # source data table
    source_dt = view_to_list(source_data)
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

    # Rename source fields to match target fields, rename id to mmjmenuid so
    # we can track related data
    new_header = {
        'id': 'mmjmenuid',
        'vendor_id': 'vendorId',
        'category_id': 'categoryId',
        'body': 'description'
    }

    renamed_header = etl.rename(menu_items, new_header)

    # Two-step transform and cut. First we need to cut the name
    # and id from the source data to map to.
    cut_source_cats = etl.cut(source_ctx, 'name', 'id')
    source_values = etl.values(cut_source_cats, 'name', 'id')
    source_product_names = etl.values(menu_items, 'name')
    source_desc = etl.values(menu_items, 'body')

    # Then we nede a dict of categories to compare against.
    # id is stored to match against when transforming and mapping categories
    source_ctx = dict([(value, id) for (value, id) in source_values])
    # find the menu item category id
    mmj_ids = etl.values(menu_items, 'category_id')

    cat_prods = category_products(menu_items, mmj_ids, source_ctx, target_ctx)
    cat_genetics = genetics_products(menu_items, target_ctx)
    cat_names = category_names(menu_items, target_ctx)
    strains = strain_names(menu_items, target_ctx)

    #print(strains)
    # merge all 3 tables into one... git'r ready for import!
    merged_data = etl.merge(cat_prods, cat_genetics, cat_names, key='id')

    #print(merged_data.lookall())


def category_products(menu_items, mmj_cat_ids, source_ctx, target_ctx):
    # dict to hold category transformations
    target_category_names = [name for name in target_ctx.distinct('name')]
    categories_dict = {}
    # little scheme to match cats for transforming proper categories
    for item in mmj_cat_ids:
        # Separates the dictionary's values in a list, finds the position of
        # the value and gets the key at that position to match the id
        # returns source category name to compare
        source_cat_name = source_ctx.keys()[source_ctx.values().index(item)]
        if source_cat_name in PLURAL_CATEGORIES:
            source_cat_name = singularize(source_cat_name)
        # first condition, if the mmj category is found in g1
        # category, we'll assign it.
        if source_cat_name in target_category_names:
            # we found a category
            # TODO when transform - assign target data it's mongo id.
            target_cat_id = target_ctx.collection.find_one(
                {'name': source_cat_name})

            categories_dict[item] = target_cat_id.get('_id')

    category_map = OrderedDict()
    category_map["id"] = "id"
    category_map["name"] = "name"
    category_map["category_id"] = "category_id", \
        lambda value: categories_dict[value]
    cat = etl.fieldmap(menu_items, category_map)
    return cat


def genetics_products(menu_items, target_ctx):
    # if mmj product genetics field contains any of the g1 category names.
    target_category_names = [name for name in target_ctx.distinct('name')]
    genetics_dict = {}
    genetics = etl.facet(menu_items, 'genetics')
    for genetic in genetics.keys():
        if genetic and genetic in target_category_names:
            target_cat_id = target_ctx.collection.find_one(
                {'name': genetic})
            split_rows = etl.select(
                menu_items, 'genetics', lambda value: value == genetic
            )
            filtered_genetics = split_rows.values('genetics')
            for filtered_genetic in filtered_genetics:
                genetics_dict[filtered_genetic] = target_cat_id.get('_id')

    genetics_map = OrderedDict()
    genetics_map["id"] = "id"
    genetics_map["name"] = "name"
    genetics_map["category_id"] = "genetics", \
        lambda value: genetics_dict[value]

    genetics_cats = etl.fieldmap(menu_items, genetics_map)
    return genetics_cats


def category_names(menu_items, target_ctx):
    target_category_names = [name for name in target_ctx.distinct('name')]
    # if mmj product name contains any of the g1 category names
    products_dict = {}
    product_names = etl.facet(menu_items, 'name')
    for name in product_names:
        if name:
            split_names = name.split(" ")
            for cat in split_names:
                if cat in target_category_names:
                    target_cat_id = target_ctx.collection.find_one(
                        {'name': cat})
                    products_dict[name] = target_cat_id.get('_id')

    products_map = OrderedDict()
    products_map["id"] = "id"
    products_map["name"] = "name"
    products_map["category_id"] = "name", \
        lambda value: products_dict[value]

    products_cats = etl.fieldmap(menu_items, products_map)
    return products_cats


def strain_names(menu_items, target_ctx):
    # calculate sativa/indica percentage, if either has more
    # than 80% choose that type as the category, otherwise make it hybrid
    target_cat_id = {}
    strain = etl.cut(menu_items, 'sativa', 'indica')
    strain_vals = etl.dicts(view_to_list(strain))
    for strain in strain_vals.__iter__():
        if strain['sativa'] > 0 or strain['indica'] > 0:
            if strain['sativa'] >= 80:
                # sativa
                category = target_ctx.collection.find_one({'name': 'Sativa'})
                print(category.get('_id'))
                target_cat_id[strain['sativa']] = category.get('_id')
            elif strain['indica'] >= 80:
                # indica
                target_cat_id = target_ctx.collection.find_one({'name': 'Indica'})
            else:
                # hybrid
                target_cat_id = target_ctx.collection.find_one({'name': 'Hybrid'})
    print(target_cat_id)

def lab_results(data):
    # Put lab results on their own as this will be its own collection later
    lab_results = etl.cut(data, *range(11, 16))
    return lab_results


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


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2])

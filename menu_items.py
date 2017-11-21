from __future__ import division, print_function, absolute_import

import MySQLdb
import sys
import pymongo
import petl as etl
import uuid
import json
import logging
import logging.handlers
import os
import urllib


from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict

from pattern.text.en import singularize

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

# sanitize categories, need a better way to do this, perhaps a stemming lib
PLURAL_CATEGORIES = ['Seeds', 'Drinks', 'Edibles']

logging.basicConfig(filename="g1-etl-menuitems.log", level=logging.INFO)
log = logging.getLogger("g1-etl-menuitems")


def extract(table_name, collection):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-101717-cluster.cluster-cmtxwpwvylo7.us-west-2.rds.amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")

    target_db = pymongo.MongoClient("mongodb://127.0.0.1:3001")

    try:
        source_data = load_db_data(source_db, table_name)
        source_ctx = load_db_data(source_db, 'categories')

        target_data = load_mongo_data(target_db, 'inventory.products')
        target_ctx = load_mongo_data(target_db, 'inventory.categories')

        transform_menu_items(source_data, source_ctx, target_ctx, target_data)

    finally:
        source_db.close()
        target_db.close()


def transform_menu_items(source_data, source_ctx, target_ctx, target_data):
    """
    Transform data
    """
    # source data table
    source_dt = view_to_list(source_data)

    # Cut out all the fields we don't need to load
    menu_items = etl.cutout(source_dt, 'menu_id', 'body_html',
                            'deduct_from_id', 'tested_by', 'medicine_amount',
                            'medicine_measurement', 'batch_number', 'barcode',
                            'custom_barcode', 'image_updated_at',
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
    # find the menu item category id
    mmj_ids = etl.values(menu_items, 'category_id')
    # conditions
    cat_prods = category_products(menu_items, mmj_ids, source_ctx, target_ctx)
    cat_genetics = genetics_products(menu_items, target_ctx)
    cat_names = category_names(menu_items, target_ctx)

    # merge all 3 tables into one; ready for import!
    merged_data = etl.merge(cat_prods, cat_genetics, cat_names, key='id')

    # create "other" category, fill in None with Other and
    # merge it in with the other data table
    other_category = {
        "_id": random_mongo_id(),
        "organizationId": "420",
        "name": "Other",
        "tax_free": "false",
        "fixtureImage": "/northernlights/images/growone-logo-pink.svg",
    }

    etl.values(merged_data, 'category_id')
    insert_id = None
    category_exists = target_ctx.collection.find_one({"name": "Other"})
    if category_exists:
        insert_id = category_exists["_id"]
    else:
        insert_id = target_ctx.collection.insert(other_category)
    data_dict = {}
    for data in etl.values(merged_data, 'category_id'):
        if data is None:
            data_dict[data] = insert_id
    product_mapping = mappings(data_dict)
    merged_data = etl.merge(merged_data,
                            etl.fieldmap(merged_data, product_mapping),
                            key='id')

    # convert from unicode to string
    merged_data = etl.convert(merged_data, 'shareOnWM', bool)
    merged_data = etl.convert(merged_data, 'category_id', str)
    merged_data = etl.convert(merged_data, 'description', str)

    images = etl.values(menu_items, 'image_file_name', 'id')
    for image in images:
        print(image)
        if image:
            print(image)
            #image = urllib.urlretrieve("http://www.digimouth.com/news/media/2011/09/google-logo.jpg", "local-filename.jpg")
            #https://wm-mmjmenu-images-production.s3.amazonaws.com/menu_items/images/{ID}/{FILENAME}

    try:
        etl.tojson(merged_data, 'g1.json', sort_keys=True, encoding="latin-1")
    except UnicodeDecodeError, e:
        log.warn("UnicodeDecodeError: ", e)

    json_items = open("g1.json")
    parsed = json.loads(json_items.read())

    for item in parsed:
        item['_id'] = random_mongo_id()
        #print("item: ", item)
        target_data.collection.insert(item)


def category_products(menu_items, mmj_cat_ids, source_ctx, target_ctx):
    # dict to hold category transformations
    target_category_names = [name for name in target_ctx.distinct('name')]
    categories_dict = {}
    # little scheme to match cats for transforming proper categories
    for item in mmj_cat_ids:
        # Separates the dictionary's values in a list, finds the position of
        # the value and gets the key at that position to match the id
        # returns source category name to compare
        source_cat_name = None
        try:
            source_cat_name = source_ctx.keys()[source_ctx.values().index(item)]
        except ValueError:
            log.warn("Value is not in the list.", item)
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
    product_mapping = mappings(categories_dict)
    cat = etl.fieldmap(menu_items, product_mapping)
    logging.info('category_id %s', cat.lookall())

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

    product_mapping = mappings(genetics_dict)
    genetics_cats = etl.fieldmap(menu_items, product_mapping)

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

    product_mapping = mappings(products_dict)
    products_cats = etl.fieldmap(menu_items, product_mapping)

    return products_cats


def strain_names(menu_items, target_ctx):
    # calculate sativa/indica percentage, if either has more
    # than 80% choose that type as the category, otherwise make it hybrid
    target_cat = {}
    strain_data = etl.cut(menu_items, 'id', 'name', 'sativa', 'indica')
    strain_vals = etl.dicts(view_to_list(strain_data))
    for strain in strain_vals.__iter__():
        if strain['sativa'] > 0 or strain['indica'] > 0:
            if strain['sativa'] >= 80:
                # sativa
                pass
            elif strain['indica'] >= 80:
                # indica
                pass
            else:
                # hybrid
                pass


def mappings(assoc_dict):
    product = {
        "id": "id",
        "shareOnWM": "0",
        "restockLevel": "0",
        "description": "body",
        "name": "name",
        "organizationId": "420", # temporary
    }
    mapping = OrderedDict(product)
    mapping["category_id"] = "category_id", \
        lambda cat_id: assoc_dict[cat_id]

    return mapping


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


def load_db_data(db, table_name):
    """
    Data extracted from source db
    """
    return etl.fromdb(db, "SELECT * from {0} LIMIT 40".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


def random_mongo_id():
    """
    Returns a random string of length 17
    """
    random = str(uuid.uuid4())
    random = random.replace("-", "")

    return random[0:17]


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2])

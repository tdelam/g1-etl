from __future__ import division, print_function, absolute_import

import petl as etl
import MySQLdb
import sys
import itertools
import pymongo

from petl.io.db import DbView
from pymongo import MongoClient
from sqlalchemy import *


# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')


def extract(table_name, show_tables=False):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                     user="root",
                     passwd="c0l3m4N",
                     db="mmjmenu_development")

    target_db = pymongo.MongoClient(
        "mongodb://127.0.0.1:3001")

    db = target_db.meteor
    pos = etl.fromdicts(db.inventory.purchase_orders.find())

    print(etl.header(pos))

    try:
        result = load_data(source_db, table_name)
        if result:
            # handle menu items first
            # @TODO - convert this to be reusable method
            transform_menu_items(result)
    finally:
        source_db.close()


def transform_menu_items(data):
    """
    Load the transformed data into the destination(s)
    +-------------+--------------+------+------+------+
    | thc_percent | thca_percent | cbn  | cbd  | cbda |
    +=============+==============+======+======+======+
    |         0.0 |          0.0 |  0.0 |  0.0 |  0.0 |
    +-------------+--------------+------+------+------+
    |         0.0 |          0.0 |  0.0 |  0.0 |  0.0 |
    +-------------+--------------+------+------+------+
    |         0.0 |          0.0 |  0.0 |  0.0 |  0.0 |
    +-------------+--------------+------+------+------+
    |         1.0 |          2.0 | 65.0 | 0.03 |  6.0 |
    +-------------+--------------+------+------+------+
    """
    data_table = dbview_to_list(data)
    
    # cut out all the fields we don't need to load
    menu_items = etl.cutout(data_table, 'menu_id', 'body_html', 'deduct_from_id', 
        'tested_by', 'medicine_amount', 'medicine_measurement', 'batch_number', 
        'barcode', 'custom_barcode', 'taxable', 'image_updated_at', 
        'consignment', 'mmjrevu_id', 'sclabs_report_id', 
        'override_global_dollars_to_points', 'dollars_to_points', 'ingredients',
        'created_at', 'updated_at', 'deleted_at', 'lab_batch_number', 
        'lab_license_number', 'activation_time', 'olcc_medical_grade', 
        'thc_percent_min', 'thca_percent_min')

    # put lab results on their own as this will be its own collection later
    lab_results = etl.cut(menu_items, *range(11, 16))

    etl.tojson(menu_items, 'dump.json', sort_keys=True)
    #print(source_count(menu_items))
    #print(open('dump.json').read())
    
    #print(etl.look(lab_results))


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

def load_mongo_data(data):
    return etl.fromdb()


def load_data(db, table_name):
    """
    Manipulate the data extracted by the previous extract method
    """
    return etl.fromdb(db, "SELECT * from {0}".format(table_name))


def dbview_to_list(data):
    if type(data) is DbView:
        # convert the db view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))


if __name__ == '__main__':
    extract(sys.argv[1])
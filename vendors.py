from __future__ import division, print_function, absolute_import

from random import randint

import sys
import MySQLdb
import pymongo
import requests
import random
import uuid
import petl as etl
import urllib2
import json
import logging
import logging.handlers

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict

from utilities import utils

logging.basicConfig(filename="g1-etl-vendors.log", level=logging.INFO)
log = logging.getLogger("g1-etl-vendors")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract():
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-101717-cluster.cluster-cmtxwpwvylo7.us-west-2.rds.amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")

    target_db = pymongo.MongoClient("mongodb://127.0.0.1:3001")

    try:
        source_data = load_db_data(source_db, 'vendors')
        source_ctx = load_db_data(source_db, 'vendors')

        target_data = load_mongo_data(target_db, 'crm.vendors')
        target_ctx = load_mongo_data(target_db, 'crm.vendors')

        transform_vendors(source_data, target_data, source_ctx, target_ctx)

    finally:
        source_db.close()
        target_db.close()


def transform_vendors(source_data, target_data, source_ctx, target_ctx):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(source_data)
    cut_data = ['id', 'dispensary_id', 'name', 'phone_number', 'email',
                'country', 'state', 'city', 'address', 'zip_code',
                'liscense_no', 'confirmed', 'website']
    vendor_data = etl.cut(source_dt, cut_data)
    vendors = (
        etl
        .addfield(vendor_data, 'uid')
        .addfield('categories')
        .addfield('categoryNames')
        .addfield('entityType')
        .addfield('unpaidBalance')
    )
    address_data = etl.dicts(
        etl.cut(vendors, 'city', 'zip_code', 'state', 'country')
    )

    vendor_mappings = OrderedDict()
    vendor_mappings['id'] = 'id'
    vendor_mappings['address'] = 'address'

    # generate uid
    vendor_mappings['uid'] = lambda _: generate_uid()

    # field renames
    vendor_mappings['accountStatus'] = 'confirmed'
    vendor_mappings['phone'] = 'phone_number'
    vendor_mappings['licenceNumber'] = 'liscense_no'

    vendors_fields = etl.fieldmap(vendors, vendor_mappings)
    merged_vendors = etl.merge(vendors, vendors_fields, key='id')

    print(etl.header(merged_vendors))

    try:
        etl.tojson(merged_vendors, 'g1-vendors.json',
                   sort_keys=True, encoding="latin-1")
    except UnicodeDecodeError, e:
        log.warn("UnicodeDecodeError: ", e)

    json_items = open("g1-vendors.json")
    parsed = json.loads(json_items.read())

    for item in parsed:
        item['_id'] = random_mongo_id()
        target_data.collection.insert(item)


def source_count(source_data):
    """
    Count the number of records from source(s)
    """
    if source_data is not None:
        return etl.nrows(source_data)
    return None


def destination_count(dest_data):
    """
    Same as source_count but with destination(s)
    """
    if dest_data is not None:
        return etl.nrows(dest_data)
    return None


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
    return etl.fromdb(db, "SELECT * from {0} LIMIT 10".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


def generate_uid():
    """
    Generates UID for G1
    """
    range_start = 10**(8 - 1)
    range_end = (10**8) - 1
    return randint(range_start, range_end)


def convert_true(field):
    True if field == 1 else False


def format_address(address, vendors):
    """
    Address format for G1
    """
    address_mappings = OrderedDict()
    address_mappings['city'] = 'city'
    address_mappings['zip'] = 'zip_code'
    address_mappings['state'] = 'state'

    vendor = etl.fieldmap(vendors, address_mappings)

    return etl.dicts(vendor)


def random_mongo_id():
    """
    Returns a random string of length 17
    """
    random = str(uuid.uuid4())
    random = random.replace("-", "")

    return random[0:17]


if __name__ == '__main__':
    extract()

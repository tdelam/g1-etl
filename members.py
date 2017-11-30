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

logging.basicConfig(filename="log/g1-etl-members.log", level=logging.INFO)
log = logging.getLogger("g1-etl-members")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

ENV = 'development'

def extract():
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    target_db = pymongo.MongoClient("mongodb://127.0.0.1:3001")

    try:
        source_data = load_db_data(source_db, 'customers')
        source_ctx = load_db_data(source_db, 'customers')

        target_data = load_mongo_data(target_db, 'crm.members')
        target_ctx = load_mongo_data(target_db, 'crm.members')

        transform_members(source_data, target_data, source_ctx, target_ctx)

    finally:
        source_db.close()
        target_db.close()


def transform_members(source_data, target_data, source_ctx, target_ctx):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(source_data)
    cut_data = [
        'id', 'dispensary_id', 'picture_file_name', 'name', 'email',
        'address', 'phone_number', 'dob', 'license_type', 'registry_no',
        'membership_id', 'given_caregivership', 'tax_exempt',
        'drivers_license_no', 'points', 'locked_visits',
        'locked_visits_reason', 'caregiver_id', 'picture_file_name'
    ]
    members = etl.cut(source_dt, cut_data)

    member_obj = {
        "id": "id",
        "uid": "uid",
        "tax_exempt": "taxExempt",
        "dispensary_id": "organizationId"
    }

    member_conversions = {
        "tax_exempt": bool,
        "dob": str
    }
    """
    Tranformations TODO:
    percentOfLimit = customers.purchases.reject {
        |obj| !(start.beginning_of_day.DateTime.now.end_of_day).cover?
            (obj.created_at) }
            .map(&:amount).sum) / customer.daily_purchase_limit) * 100).to_i
    """
    members = etl.addfield(members, 'uid')
    members_uid = etl.convert(members, 'uid', lambda _: generate_uid())

    pictures = {}
    pics = etl.values(members_uid, 'picture_file_name', 'id')
    pics_dict = dict([(value, id) for (value, id) in pics])
    for pic, user_id in pics_dict.iteritems():
        if user_id and pic:
            # Download images for user. ENV is development/production.
            utils.download_images(ENV, user_id, pic)
            pictures[user_id] = pic
    # set picture for user id and map fields
    member_mapping = OrderedDict(member_obj)
    member_mapping["picture_file_name"] = "id", lambda image: pictures[image]
    #member_mapping = mappings(pictures)
    mapped_table = etl.fieldmap(members_uid, member_mapping)

    # transform some fields and merge mapped tables
    merged_data = etl.merge(members, mapped_table, key='id')
    merged_data = etl.convert(merged_data, member_conversions)

    final_data = etl.rename(merged_data, member_obj)
    print(final_data.lookall())
    try:
        etl.tojson(merged_data, 'g1-members.json',
                   sort_keys=True, encoding="latin-1")
    except UnicodeDecodeError, e:
        log.warn("UnicodeDecodeError: ", e)

    json_items = open("g1-members.json")
    parsed = json.loads(json_items.read())

    for item in parsed:
        item['_id'] = random_mongo_id()
        print("item: ", item)
        #target_data.collection.insert(item)


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
    return etl.fromdb(db, "SELECT * from {0}".format(table_name))


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


def random_mongo_id():
    """
    Returns a random string of length 17
    """
    random = str(uuid.uuid4())
    random = random.replace("-", "")

    return random[0:17]

if __name__ == '__main__':
    extract()

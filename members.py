from __future__ import division, print_function, absolute_import

from random import randint

import sys
import MySQLdb
import pymongo
import requests
import random
import petl as etl
import urllib2
import logging
import logging.handlers

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict

from utilities import utils

logging.basicConfig(filename="g1-etl-members.log", level=logging.INFO)
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
        'drivers_license_no', 'points', 'card_expires_at', 'locked_visits',
        'locked_visits_reason', 'caregiver_id', 'picture_file_name'
    ]
    members = etl.cut(source_dt, cut_data)
    """
    Tranformations TODO:
    1. generate uid
    2. download picture_file_name and store them
    3. percentOfLimit = customers.purchases.reject {
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
            utils.download_images(ENV, user_id, pic)


def source_count(data):
    """
    Count the number of records from source(s)
    """
    if data is not None:
        return etl.nrows(data)
    return None


def destination_count():
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

def generate_uid():
    """
    Generates UID for G1
    """
    range_start = 10**(8 - 1)
    range_end = (10**8) - 1
    return randint(range_start, range_end)

if __name__ == '__main__':
    extract()

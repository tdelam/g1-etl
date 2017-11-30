from __future__ import division, print_function, absolute_import

from random import randint

import sys
import MySQLdb
import pymongo
import requests
import random
import uuid
import petl as etl
import json
import logging
import logging.handlers

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict

from utilities import utils, g1_jwt

logging.basicConfig(filename="logs/g1-etl-members.log", level=logging.INFO)
log = logging.getLogger("g1-etl-members")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

ENV = 'development'

# Defaults to be changed when we have this information
STATUS_CODE = 200  # TODO: change to capture status code from API

URL = 'http://localhost:3004/api/mmjetl/load/members'


def extract(token, organization_id):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    try:
        source_data = load_db_data(source_db, 'customers')
        transform_members(source_data, token, organization_id)

    finally:
        source_db.close()


def transform_members(source_data, token, organization_id):
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

    try:
        etl.tojson(merged_data, 'g1-members-{0}.json'.format(organization_id),
                   sort_keys=True, encoding="latin-1")
    except UnicodeDecodeError, e:
        log.warn("UnicodeDecodeError: ", e)

    json_items = open("g1-members-{0}.json".format(organization_id))
    parsed_members = json.loads(json_items.read())
    
    members = []
    for item in parsed_members:
        # set up final structure for API
        members.append(item)        

    headers = {'Authorization': 'Bearer {0}'.format(token)}
    
    for item in utils.chunks(members, 5):
        if STATUS_CODE == 200:
            # Do something with chunked data
            print(json.dumps(item))
            # r = requests.post(URL, data=item, headers=headers)
        else:
            logging.warn('Chunk has failed: {0}'.format(item))


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
    extract(g1_jwt.jwt_encode(), sys.argv[1])

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
from datetime import date, datetime
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
        'locked_visits_reason', 'caregiver_id', 'picture_file_name',
        'card_expires_at', 'created_at', 'updated_at', 'physician_id',
        'custom_membership_id', 'organization_membership_id'
    ]
    member_data = etl.cut(source_dt, cut_data)

    members = (
        etl
        .addfield(member_data, 'identificationType')
        .addfield('createdAtEpoch')
    )

    member_mapping = OrderedDict()

    member_mapping['id'] = 'id'
    member_mapping['caregiver_id'] = 'caregiver_id'
    member_mapping['dispensary_id'] = 'dispensary_id'
    member_mapping['physician_id'] = 'physician_id'
    member_mapping['custom_membership_id'] = 'custom_membership_id'
    member_mapping['organization_membership_id'] = 'organization_membership_id'
    member_mapping['picture_file_name'] = 'picture_file_name'
    member_mapping['organizationId'] = 'organization_id'
    member_mapping['dateOfBirth'] = 'dob'
    member_mapping['memberType'] = 'license_type'
    member_mapping['mmjCard'] = 'registry_no'
    member_mapping['membershipLevel'] = 'membership_id'
    member_mapping['isCaregiver'] = 'given_caregivership'
    member_mapping['identificationNumber'] = 'drivers_license_no'
    member_mapping['points'] = 'points'
    member_mapping['expiryDate'] = 'card_expires_at'
    member_mapping['taxExempt'] = 'tax_exempt'
    member_mapping['locked_visits'] = 'locked_visits'
    member_mapping['locked_visits_reason'] = 'accountStatusNotes'
    member_mapping['createdAt'] = 'created_at'
    member_mapping['updatedAt'] = 'updated_at'

    f = lambda x: print(x)
    
    member_mapping['accountStatus'] = 'accountStatus', \
        lambda x: print(x)

    member_mapping['taxExempt'] = 'taxExempt', \
        lambda x: True if x.taxExempt == 1 else False

    member_fields = etl.fieldmap(members, member_mapping)

    #merged_members = etl.merge(members, member_fields, key='id')
    # pictures = {}
    # pics = etl.values(members_uid, 'picture_file_name', 'id')
    # pics_dict = dict([(value, id) for (value, id) in pics])

    # for pic, user_id in pics_dict.iteritems():
    #     if user_id and pic:
    #         # Download images for user. ENV is development/production.
    #         utils.download_images(ENV, user_id, pic)
    #         pictures[user_id] = pic
    # print(pictures)

    # member_mapping["picture_file_name"] = "id", lambda image: pictures[image]
    # mapped_table = etl.fieldmap(members_uid, member_mapping)

    # transform some fields and merge mapped tables
    # merged_data = etl.merge(members, mapped_table, key='id')

    # final_data = etl.rename(merged_data, member_obj)

    try:
        etl.tojson(member_fields, 'g1-members-{0}.json'
                   .format(organization_id),
                   sort_keys=True, encoding="latin-1", default=json_serial)
    except UnicodeDecodeError, e:
        log.warn("UnicodeDecodeError: ", e)

    json_items = open("g1-members-{0}.json".format(organization_id))
    parsed_members = json.loads(json_items.read())
    
    members = []
    for item in parsed_members:
        item['mmjKeys'] = {
            'caregiver_id': item['caregiver_id'],
            'dispensary_id': item['dispensary_id'],
            'physician_id': item['physician_id'],
            'custom_membership_id': item['custom_membership_id'],
            'organization_membership_id': item['organization_membership_id'],
            'picture_file_name': item['picture_file_name']
        }
        # set up final structure for API
        item['identificationType'] = 'Drivers License'
        print(json.dumps(item))
        members.append(item)

    #print(members)

    headers = {'Authorization': 'Bearer {0}'.format(token)}
    
    for item in utils.chunks(members, 5):
        if STATUS_CODE == 200:
            # Do something with chunked data
            pass
            #print(json.dumps(item))
            # r = requests.post(URL, data=item, headers=headers)
        else:
            logging.warn('Chunk has failed: {0}'.format(item))


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


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
    return etl.fromdb(db, "SELECT * from {0} limit 1".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(g1_jwt.jwt_encode(), sys.argv[1])

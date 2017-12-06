from __future__ import division, print_function, absolute_import

from random import randint

#import jwt
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


logging.basicConfig(filename="logs/g1-etl-vendors.log", level=logging.INFO)
log = logging.getLogger("g1-etl-vendors")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')

URL = 'http://localhost:3004/api/mmjetl/load/vendors'
# Defaults to be changed when we have this information
STATUS_CODE = 200  # TODO: change to capture status code from API


def extract(organization_id):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    try:
        source_data = load_db_data(source_db, 'vendors')
        transform_vendors(source_data, organization_id)

    finally:
        source_db.close()


def transform_vendors(source_data, organization_id):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(source_data)
    cut_data = ['id', 'dispensary_id', 'mmjvenu_id', 'name', 'phone_number',
                'email', 'country', 'state', 'city', 'address', 'zip_code',
                'liscense_no', 'confirmed', 'website']
    vendor_data = etl.cut(source_dt, cut_data)
    vendors = (
        etl
        .addfield(vendor_data, 'organizationId')
        .addfield('mmKeys')
    )

    vendor_mappings = OrderedDict()
    vendor_mappings['id'] = 'id'
    vendor_mappings['dispensary_id'] = 'dispensary_id'
    vendor_mappings['address'] = 'address'

    # field renames
    vendor_mappings['accountStatus'] = \
        lambda x: "ACTIVE" if x.confirmed == 1 else "INACTIVE"
    vendor_mappings['phone'] = 'phone_number'
    vendor_mappings['licenceNumber'] = 'liscense_no'
    vendor_mappings['zip'] = 'zip_code'
    vendor_mappings['organizationId'] = organization_id

    vendors_fields = etl.fieldmap(vendors, vendor_mappings)
    merged_vendors = etl.merge(vendors, vendors_fields, key='id')

    #try:
    #    etl.tojson(merged_vendors, 'g1-vendors-{0}.json'
    #               .format(organization_id),
    #               sort_keys=True, encoding="latin-1")
    #except UnicodeDecodeError, e:
    #    log.warn("UnicodeDecodeError: ", e)

    #json_items = open("g1-vendors-{0}.json".format(organization_id))
    #parsed_vendors = json.loads(json_items.read())

    vendors = []
    for item in merged_vendors:
        item['address'] = {
            'line1': item['address'],
            'line2': None,
            'city': item['city'],
            'state': item['state'],
            'zip': item['zip'],
            'country': item['country'],
        }

        item['phone'] = [{
            'name': 'business',
            'number': item['phone'],
            'default': True
        }]

        item['mmjKeys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id'],
            'mmjvenu_id': item['mmjvenu_id']
        }

        # mutate dict and remove fields that are mapped and no longer required
        del item['zip']
        del item['state']
        del item['country']
        del item['city']
        del item['zip_code']
        del item['phone_number']
        del item['confirmed']
        del item['liscense_no']
        # delete fk's
        del item['mmjvenu_id']
        del item['id']
        del item['dispensary_id']

        # set up final structure for API
        vendors.append(item)

    result = json.dumps(vendors, sort_keys=True, indent=4, default=json_serial)
    #print(result)
    return result

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


def load_db_data(db, table_name):
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


if __name__ == '__main__':
    extract(sys.argv[1])

from __future__ import division, print_function, absolute_import

import sys
import MySQLdb
import pymongo
import petl as etl
import json

from collections import OrderedDict
from utilities import utils


# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract(organization_id, debug):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-10171"
                                "7-cluster.cluster-cmtxwpwvylo7.us-west-2.rds"
                                ".amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")
    try:
        source_data = utils.load_db_data(source_db, 'vendors')
        return transform(source_data, organization_id, debug)
    finally:
        source_db.close()


def transform(source_data, organization_id, debug):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(source_data)
    cut_data = ['id', 'dispensary_id', 'mmjvenu_id', 'name', 'phone_number',
                'email', 'country', 'state', 'city', 'address', 'zip_code',
                'liscense_no', 'confirmed', 'website']
    vendor_data = etl.cut(source_dt, cut_data)
    vendors = (
        etl
        .addfield(vendor_data, 'organizationId')
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

    vendors = []
    for item in etl.dicts(merged_vendors):
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

        item['keys'] = {
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

    if debug:
        result = json.dumps(vendors, sort_keys=True, indent=4,
                            default=utils.json_serial)
        print(result)

    return vendors


if __name__ == '__main__':
    extract(sys.argv[1], True)

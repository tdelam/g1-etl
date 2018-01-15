from __future__ import division, print_function, absolute_import

import os
import sys
import inspect
import MySQLdb
import petl as etl
import json

from collections import OrderedDict

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe()))
)
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from utilities import utils

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract(dispensary_id, organization_id, debug):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-011218"
                                     ".cmtxwpwvylo7.us-west-2.rds.amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")
    try:
        source_data = utils.load_db_data(source_db, dispensary_id, 'vendors')
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

    vendors_fields = etl.fieldmap(vendor_data, vendor_mappings)
    merged_vendors = etl.merge(vendor_data, vendors_fields, key='id')

    vendors = []
    for item in etl.dicts(merged_vendors):
        if item['address'] is not None:
            item['address'] = {
                'line1': item['address'],
                'line2': None,
                'city': item['city'],
                'state': item['state'],
                'zip': item['zip'],
                'country': item['country'],
            }
        else:
            del item['address']

        if item['licenceNumber'] is None or item['email'] is None or item['website'] is None:
            del item['licenceNumber']
            del item['email']
            del item['website']

        if item['phone'] is not None:
            item['phone'] = [{
                'name': 'business',
                'number': item['phone'],
                'default': True
            }]
        else:
            del item['phone']

        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id'],
            'mmjvenu_id': item['mmjvenu_id']
        }

        # remove any item['keys'] tuples with None values
        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

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
    extract(sys.argv[1], sys.argv[2], True)

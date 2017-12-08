from __future__ import division, print_function, absolute_import

import os,sys,inspect
import MySQLdb
import petl as etl
import json

currentdir = os.path.dirname(os.path.abspath(inspect.getfile(inspect.currentframe())))
parentdir = os.path.dirname(currentdir)
sys.path.insert(0,parentdir)

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
        source_data = utils.load_db_data(source_db, 'physicians')
        return transform(source_data, organization_id, debug)

    finally:
        source_db.close()


def transform(source_data, organization_id, debug):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(source_data)
    cut_data = ['id', 'dispensary_id', 'name', 'email', 'created_at',
                'updated_at', 'address', 'city', 'state', 'country',
                'zip_code', 'website', 'license_no', 'phone_number']
    physician_data = etl.cut(source_dt, cut_data)

    physicians = (
        etl
        .addfield(physician_data, 'organizationId')
        .addfield('createdAtEpoch')
    )

    physician_mapping = OrderedDict()

    physician_mapping['id'] = 'id'
    physician_mapping['dispensary_id'] = 'dispensary_id'
    physician_mapping['name'] = 'name'
    physician_mapping['email'] = 'email'
    physician_mapping['createdAt'] = 'created_at'
    physician_mapping['updatedAt'] = 'updated_at'
    physician_mapping['address'] = 'address'
    physician_mapping['city'] = 'city'
    physician_mapping['state'] = 'state'
    physician_mapping['zip_code'] = 'zip_code'
    physician_mapping['country'] = 'country'
    physician_mapping['verificationWebsite'] = 'website'
    physician_mapping['licenceNumber'] = 'license_no'
    physician_mapping['phone'] = 'phone_number'
    physician_mapping['organizationId'] = organization_id

    physician_fields = etl.fieldmap(physicians, physician_mapping)

    physicians = []
    for item in etl.dicts(physician_fields):
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id']
        }

        item['address'] = {
            'line1': item['address'],
            'city': item['city'],
            'state': item['state'],
            'zip': item['zip_code'],
            'country': item['country'],
        }

        item['phone'] = [{
            'name': 'work',
            'number': item['phone'],
            'default': True
        }]

        del item['city']
        del item['zip_code']
        del item['state']
        del item['country']
        del item['dispensary_id']
        del item['id']

        physicians.append(item)

    if debug:
        result = json.dumps(physicians, sort_keys=True,
                            indent=4, default=utils.json_serial)
        print(result)

    return physicians


if __name__ == '__main__':
    extract(sys.argv[1], True)

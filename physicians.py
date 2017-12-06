from __future__ import division, print_function, absolute_import

import sys
import MySQLdb
import pymongo
import petl as etl
import json
import logging
import logging.handlers

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict
from utilities import utils, g1_jwt

logging.basicConfig(filename="logs/g1-etl-physicians.log", level=logging.INFO)
log = logging.getLogger("g1-etl-physicians")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract(organization_id):
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
        source_data = load_db_data(source_db, 'physicians')
        transform(source_data, organization_id)

    finally:
        source_db.close()


def transform(source_data, organization_id):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(source_data)
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

    result = json.dumps(physicians, sort_keys=True,
                        indent=4, default=utils.json_serial)
    return result


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


def load_db_data(db, table_name, from_json=False):
    """
    Data extracted from source db
    """
    return etl.fromdb(db, "SELECT * from {0} limit 15".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(sys.argv[1])

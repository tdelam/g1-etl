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
from datetime import date, datetime
from utilities import utils

logging.basicConfig(filename="logs/g1-etl-members.log", level=logging.INFO)
log = logging.getLogger("g1-etl-members")

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
        source_data = load_db_data(source_db, 'customers')
        return transform(source_data, organization_id)
    finally:
        source_db.close()


def transform(source_data, organization_id):
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
        'custom_membership_id', 'organization_membership_id', 'city',
        'state', 'zip_code', 'address', 'organization_id'
    ]
    member_data = etl.cut(source_dt, cut_data)

    members = (
        etl
        .addfield(member_data, 'identificationType')
        .addfield('createdAtEpoch')
        .addfield('organizationId')
    )

    member_mapping = OrderedDict()

    member_mapping['id'] = 'id'
    member_mapping['caregiver_id'] = 'caregiver_id'
    member_mapping['dispensary_id'] = 'dispensary_id'
    member_mapping['physician_id'] = 'physician_id'
    member_mapping['custom_membership_id'] = 'custom_membership_id'
    member_mapping['organization_membership_id'] = 'organization_membership_id'
    member_mapping['picture_file_name'] = 'picture_file_name'
    member_mapping['dateOfBirth'] = 'dob'
    member_mapping['name'] = 'name'
    member_mapping['email'] = 'email'
    member_mapping['organization_id'] = 'organization_id'
    # MEDICAL 1, RECREATIONAL 2
    member_mapping['memberType'] = \
        lambda m: 'MEDICAL' if m.license_type == 1 else 'RECREATIONAL'

    member_mapping['mmjCard'] = 'registry_no'
    member_mapping['membershipLevel'] = 'membership_id'
    member_mapping['isCaregiver'] = \
        lambda x: True if x.given_caregivership == 1 else False
    member_mapping['identificationNumber'] = 'drivers_license_no'
    member_mapping['points'] = 'points'
    member_mapping['expiryDate'] = 'card_expires_at'
    member_mapping['taxExempt'] = \
        lambda x: True if x.tax_exempt == 1 else False
    member_mapping['locked_visits'] = 'locked_visits'
    member_mapping['locked_visits_reason'] = 'accountStatusNotes'
    member_mapping['address'] = 'address'
    member_mapping['city'] = 'city'
    member_mapping['zip_code'] = 'zip_code'
    member_mapping['state'] = 'state'
    member_mapping['createdAt'] = 'created_at'
    member_mapping['updatedAt'] = 'updated_at'
    member_mapping['organizationId'] = organization_id

    member_mapping['accountStatus'] = \
        lambda x: 'INACTIVE' if x.locked_visits == 1 else "ACTIVE"

    member_mapping['taxExempt'] = 'taxExempt', \
        lambda x: True if x.taxExempt == 1 else False

    member_fields = etl.fieldmap(members, member_mapping)

    members = []
    for item in etl.dicts(member_fields):
        item['keys'] = {
            'caregiver_id': item['caregiver_id'],
            'dispensary_id': item['dispensary_id'],
            'physician_id': item['physician_id'],
            'custom_membership_id': item['custom_membership_id'],
            'organization_membership_id': item['organization_membership_id'],
            'picture_file_name': item['picture_file_name'],
            'organization_id': item['organization_id'],
        }
        # set up final structure for API
        item['identificationType'] = 'Drivers License'

        # We may not need this in the data
        item['address'] = {
            'line1': item['address'],
            'city': item['city'],
            'state': item['state'],
            'zip': item['zip_code'],
        }

        del item['address']
        del item['city']
        del item['zip_code']
        del item['state']
        del item['dispensary_id']
        del item['id']
        del item['physician_id']
        del item['caregiver_id']
        del item['custom_membership_id']
        del item['organization_membership_id']
        del item['organization_id']

        members.append(item)

    result = json.dumps(members, sort_keys=True, indent=4, default=json_serial)
    print(result)
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

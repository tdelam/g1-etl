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
    source_db = MySQLdb.connect(host="mmjmenu-production-copy-playground-10171"
                                "7-cluster.cluster-cmtxwpwvylo7.us-west-2.rds"
                                ".amazonaws.com",
                                user="mmjmenu_app",
                                passwd="V@e67dYBqcH^U7qVwqPS",
                                db="mmjmenu_production")
    try:
        source_data = utils.load_db_data(source_db, dispensary_id, 'customers')
        return transform(source_data, organization_id, debug)
    finally:
        source_db.close()


def transform(source_data, organization_id, debug):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(source_data)

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
    member_mapping['isCaregiver'] = \
        lambda x: utils.true_or_false(x.given_caregivership)
    member_mapping['identificationNumber'] = 'drivers_license_no'
    member_mapping['points'] = 'points'
    member_mapping['expiryDate'] = 'card_expires_at'
    member_mapping['taxExempt'] = lambda x: utils.true_or_false(x.tax_exempt)
    member_mapping['locked_visits'] = 'locked_visits'
    member_mapping['locked_visits_reason'] = 'accountStatusNotes'
    member_mapping['address'] = 'address'
    member_mapping['city'] = 'city'
    member_mapping['zip_code'] = 'zip_code'
    member_mapping['state'] = 'state'
    member_mapping['createdAt'] = 'created_at'
    member_mapping['updatedAt'] = 'updated_at'

    member_mapping['accountStatus'] = \
        lambda x: utils.account_status(x.locked_visits)

    member_fields = etl.fieldmap(members, member_mapping)

    members = []
    for item in etl.dicts(member_fields):
        item['keys'] = {
            'id': item['id'],
            'caregiver_id': item['caregiver_id'],
            'dispensary_id': item['dispensary_id'],
            'physician_id': item['physician_id'],
            'custom_membership_id': item['custom_membership_id'],
            'organization_membership_id': item['organization_membership_id'],
            'picture_file_name': item['picture_file_name'],
            'organization_id': item['organization_id'],
        }

        # remove any item['keys'] tuples with None values
        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

        # set up final structure for API
        item['identificationType'] = 'Drivers License'

        # We may not need this in the data
        item['address'] = [{
            'line1': item['address'],
            'city': item['city'],
            'state': item['state'],
            'zip': item['zip_code'],
        }]

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
        del item['picture_file_name']
        del item['locked_visits_reason']
        del item['locked_visits']

        members.append(item)

    if debug:
        result = json.dumps(members, sort_keys=True,
                            indent=4, default=utils.json_serial)
        print(result)

    return members


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2], True)

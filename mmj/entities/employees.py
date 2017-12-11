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
        mmj_employees = utils.load_db_data(source_db, 'users')
        mmj_dispensary_users = utils.load_db_data(source_db,
                                                  'dispensary_users')
        return transform(mmj_employees, mmj_dispensary_users,
                         organization_id, debug)

    finally:
        source_db.close()


def transform(mmj_employees, mmj_dispensary_users, organization_id, debug):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(mmj_employees)
    roles_dt = utils.view_to_list(mmj_dispensary_users)

    cut_data = ['id', 'email', 'first_name', 'organization_id',
                'last_name', 'created_at', 'updated_at']

    cut_dispensary_users = ['id', 'access', 'active']

    employee_data = etl.cut(source_dt, cut_data)
    roles_data = etl.cut(roles_dt, cut_dispensary_users)

    employees = (
        etl
        .addfield(employee_data, 'keys')
        .addfield('name')
        .addfield('role')
    )

    lookup_role = etl.lookup(roles_data, 'id', 'access')
    lookup_active = etl.lookup(roles_data, 'id', 'active')

    mappings = OrderedDict()
    mappings['id'] = 'id'
    mappings['email'] = 'email'
    mappings['name'] = \
        lambda name: "{0} {1}".format(name.first_name, name.last_name)

    """
    Roles:
        1 = site-admin
        2 = site-admin
        3 = store-manager
        4 = budtender
    """
    mappings['role'] = lambda x: assign_role(lookup_role[x.id][0])

    mappings['createdAt'] = 'created_at'
    mappings['updatedAt'] = 'updated_at'
    mappings['organization_id'] = 'organization_id'  # keep mmj org
    mappings['accountStatus'] = \
        lambda x: "ACTIVE" if lookup_active[x.id][0] == 1 else "INACTIVE"

    fields = etl.fieldmap(employees, mappings)
    merged_employees = etl.merge(employees, fields, key='id')

    mapped_employees = []
    for item in etl.dicts(merged_employees):
        item['keys'] = {
            'id': item['id'],
            'organization_id': item['organization_id']
        }
        del item['first_name']
        del item['last_name']
        del item['created_at']
        del item['id']
        del item['organization_id']
        # set up final structure for API
        mapped_employees.append(item)

    if debug:
        result = json.dumps(mapped_employees, sort_keys=True,
                            indent=4, default=utils.json_serial)
        print(result)

    return mapped_employees


def assign_role(id):
    if id == 1 or id == 2:
        return 'site-admin'
    elif id == 3:
        return 'store-manager'
    else:
        return 'budtender'


if __name__ == '__main__':
    extract(sys.argv[1], True)

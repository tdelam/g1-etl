from __future__ import division, print_function, absolute_import

import sys
import MySQLdb
import petl as etl
import json
import logging
import logging.handlers

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView
from collections import OrderedDict
from utilities import utils
from datetime import date, datetime


logging.basicConfig(filename="logs/g1-etl-employees.log", level=logging.INFO)
log = logging.getLogger("g1-etl-employees")

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract(organization_id):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    try:
        mmj_employees = load_db_data(source_db, 'users')
        mmj_dispensary_users = load_db_data(source_db, 'dispensary_users')

        transform(mmj_employees, mmj_dispensary_users, organization_id)

    finally:
        source_db.close()


def transform(mmj_employees, mmj_dispensary_users, organization_id):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(mmj_employees)
    roles_dt = view_to_list(mmj_dispensary_users)

    cut_data = ['id', 'email', 'first_name', 
                'last_name', 'created_at', 'updated_at']
    
    cut_dispensary_users = ['id', 'access', 'active']

    employee_data = etl.cut(source_dt, cut_data)
    roles_data = etl.cut(roles_dt, cut_dispensary_users)

    employees = (
        etl
        .addfield(employee_data, 'organizationId')
        .addfield('keys')
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
    mappings['role'] = lambda x: lookup_role[x.id][0]
    mappings['createdAt'] = 'created_at'
    mappings['updatedAt'] = 'updated_at'
    mappings['organizationId'] = organization_id
    mappings['accountStatus'] = \
        lambda x: "ACTIVE" if lookup_active[x.id][0] == 1 else "INACTIVE"

    fields = etl.fieldmap(employees, mappings)
    merged_employees = etl.merge(employees, fields, key='id')
    
    mapped_employees = []
    for item in etl.dicts(merged_employees):
        item['keys'] = {
            'id': item['id']
        }
        del item['first_name']
        del item['last_name']
        # set up final structure for API
        mapped_employees.append(item)

    print(json.dumps(mapped_employees, default=json_serial))


def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))


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

from __future__ import division, print_function, absolute_import

import os
import sys
import inspect
import MySQLdb
import petl as etl
import json
import datetime 

from collections import OrderedDict
from faker import Faker

currentdir = os.path.dirname(os.path.abspath(
    inspect.getfile(inspect.currentframe()))
)
parentdir = os.path.dirname(currentdir)
sys.path.insert(0, parentdir)

from utilities import utils

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('latin-1')


def extract(dispensary_id, organization_id, debug, fake_email):
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
        mmj_employees = utils.load_employees(source_db, dispensary_id)

        return transform(mmj_employees,
                         organization_id, debug, fake_email, source_db)

    finally:
        source_db.close()


def transform(mmj_employees, organization_id, debug, fake_email, source_db):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(mmj_employees)
    cut_data = ['id', 'email', 'first_name', 'organization_id',
                'last_name', 'created_at', 'updated_at', 'login']

    employee_data = etl.cut(source_dt, cut_data)

    employees = (
        etl
        .addfield(employee_data, 'keys')
        .addfield('name')
        .addfield('role')
        .addfield('dateOfBirth')
    )

    mappings = OrderedDict()
    mappings['id'] = 'id'
    mappings['name'] = \
        lambda name: _set_name(name.first_name, name.last_name, name.login)

    """
    Roles:
        1 = site-admin
        2 = site-admin
        3 = store-manager
        4 = budtender
    """
    mappings['role'] = lambda x: _assign_role(x.id, source_db)

    mappings['createdAt'] = 'created_at'
    mappings['updatedAt'] = 'updated_at'
    mappings['dateOfBirth'] = \
        lambda _: datetime.datetime(year=1970, month=01, 
                                    day=01, hour=02, minute=30)
    mappings['organization_id'] = 'organization_id'  # keep mmj org
    mappings['accountStatus'] = lambda x: _active(x.id, source_db)

    fields = etl.fieldmap(employees, mappings)
    merged_employees = etl.merge(employees, fields, key='id')

    mapped_employees = []
    for item in etl.dicts(merged_employees):
        item['keys'] = {
            'id': item['id'],
            'organization_id': item['organization_id']
        }
        
        # remove any item['keys'] tuples with None values
        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

        item['email'] = _set_email(item['email'], fake_email, debug)

        del item['login']
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


def _set_name(first_name, last_name, login):
    """
    sets the name of the employee. If first and last name are blank
    we're going to use the login 
    """
    if first_name is None and last_name is None:
        return "{0}".format(login)
    else:
        return "{0} {1}".format(first_name, last_name)


def _set_email(user_email, fake_email, debug):
    if fake_email is True and debug is True:
        fake = Faker()
        return fake.safe_email()
    else:
        return user_email


def _active(id, source_db):
    """
    This exists because the 'active' field is on the dispensary_users table
    in MMJ. The extract method queries the 'users' table. We have no way
    to know which user_id to use because our util script only loads from
    the sources limit 10 when we need to query related table by user_id
    """
    sql = ("SELECT DISTINCT active, user_id "
           "FROM dispensary_users "
           "WHERE user_id={0}").format(id)

    data = etl.fromdb(source_db, sql) 
    try:
        lookup_active = etl.lookup(data, 'user_id', 'active')
        if lookup_active[id][0] == 1:
            return 'ACTIVE'
    except KeyError:
        return "INACTIVE"


def _assign_role(id, source_db):
    """
    This exists because the 'access' field is on the dispensary_users table
    in MMJ. The extract method queries the 'users' table. We have no way
    to know which user_id to use because our util script only loads from
    the sources limit 10 when we need to query related table by user_id
    """
    sql = ("SELECT DISTINCT access, user_id "
           "FROM dispensary_users "
           "WHERE user_id={0}").format(id)

    data = etl.fromdb(source_db, sql)    
    try:
        role = etl.lookup(data, 'user_id', 'access')
        role_id = role[id][0]
        if role_id == 1 or role_id == 2:
            return 'site-admin'
        elif id == 3:
            return 'store-manager'
        else:
            return 'budtender'
    except KeyError:
        return 'budtender'  # only gets here if we get a null


if __name__ == '__main__':
    """
    !! WARNING !!
    MAKE SURE YOU DO NOT SET THE ARGS TO FALSE IN DEVELOPMENT MODE!!!
    """
    extract(sys.argv[1], sys.argv[2], True, True)

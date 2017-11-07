from __future__ import division, print_function, absolute_import

import MySQLdb
import sys
import itertools
import pymongo
import re
import petl as etl

from petl.io.db import DbView
from petl.io.json import DictsView
from petl.transform.basics import CutView

from pymongo import MongoClient
from sqlalchemy import *

from pattern.text.en import singularize

# handle characters outside of ascii
reload(sys)
sys.setdefaultencoding('utf8')


def extract(table_name, collection):
    """
    Grab all data from source(s).
    """
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")

    target_db = pymongo.MongoClient("mongodb://127.0.0.1:3001")

    try:
        source_data = load_db_data(source_db, table_name)
        source_ctx = load_db_data(source_db, 'members')

        target_data = load_mongo_data(target_db, '{0}'.format(collection))
        target_ctx = load_mongo_data(target_db, 'crm.members')

        transform_members(source_data, target_data, source_ctx, target_ctx)

    finally:
        source_db.close()
        target_db.close()


def transform_members(source_data, target_data, source_ctx, target_ctx):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = view_to_list(source_data)


def source_count(data):
    """
    Count the number of records from source(s)
    """
    if data is not None:
        return etl.nrows(data)
    return None


def destination_count(self):
    """
    Same as source_count but with destination(s)
    """
    pass


def load_mongo_data(db, collection):
    """
    Data extracted from target mongo for diff
    """
    # data from mongo needs to be treated differently.
    mongo_db = db.meteor
    return mongo_db[collection].find()


def load_db_data(db, table_name, from_json=False):
    """
    Data extracted from source db
    """
    return etl.fromdb(db, "SELECT * from {0}".format(table_name))


def view_to_list(data):
    if type(data) is DbView or type(data) is CutView:
        # convert the view to a lists of lists for petl
        # map is quicker than list comp
        return list(map(list, data))

    if type(data) is DictsView:
        return data


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2])

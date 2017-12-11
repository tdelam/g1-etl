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
        source_data = utils.load_db_data(source_db, 'dispensary_details')
        return transform(source_data, organization_id, debug)
    finally:
        source_db.close()


def transform(source_data, organization_id, debug):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    source_dt = utils.view_to_list(source_data)
    cut_data = ['id', 'dispensary_id']
    settings_data = etl.cut(source_dt, cut_data)
    settings = (
        etl
        .addfield(settings_data, 'organizationId')
    )

    mappings = OrderedDict()
    mappings['id'] = 'id'
    mappings['dispensary_id'] = 'dispensary_id'

    # field renames
    mappings['organizationId'] = organization_id

    settings_fields = etl.fieldmap(settings, mappings)
    merged_settings = etl.merge(settings, settings_fields, key='id')

    settings = []
    for item in etl.dicts(merged_settings):
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id']
        }

        # delete fk's
        del item['id']
        del item['dispensary_id']

        # set up final structure for API
        settings.append(item)

    if debug:
        result = json.dumps(settings, sort_keys=True, indent=4,
                            default=utils.json_serial)
        print(result)

    return settings


if __name__ == '__main__':
    extract(sys.argv[1], True)

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
        dispensary_details = utils.load_db_data(source_db,
                                                'dispensary_details')
        taxes = utils.load_db_data(source_db, 'taxes')
        return transform(dispensary_details, taxes,organization_id, debug,
                         source_db)
    finally:
        source_db.close()


def transform(dispensary_details, taxes, organization_id, debug, source_db):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    general_settings = utils.view_to_list(dispensary_details)
    dispensary_cut_data = ['id', 'dispensary_id', 'menu_show_tax',
                           'logo_file_name', 'inactivity_logout',
                           'calculate_even_totals',
                           'default_customer_license_type',
                           'require_customer_referrer']

    dispensary_settings_data = etl.cut(general_settings, dispensary_cut_data)
    settings = (
        etl
        .addfield(dispensary_settings_data, 'organizationId')
    )

    mappings = OrderedDict()
    mappings['id'] = 'id'

    # field renames
    mappings['organizationId'] = organization_id

    settings_fields = etl.fieldmap(settings, mappings)
    merged_settings = (
        etl
        .merge(settings, settings_fields, key='id')
        .rename({
            # Global -> General -> SESSION TIMEOUT DURATION
            'inactivity_logout': 'sessionTimeourDuration',
            # Global -> Logo
            'logo_file_name': 'image',
            # <Location> -> Sales -> TAXES IN
            'menu_show_tax': 'enableTaxesIn',
            # <Location> -> Sales -> PRICE ROUNDING
            'calculate_even_totals': 'hasPriceRounding',
            'default_customer_license_type': 'memberType',
            # <Location> -> Members -> REFERRER REQUIRED
            'require_customer_referrer': 'mandatoryReferral'
        })
    )
    settings = []
    for item in etl.dicts(merged_settings):
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id']
        }

        item['memberType'] = _member_type(item['memberType'])

        # sales.settings.taxes
        for tax in _get_taxes(item['dispensary_id'], source_db):
            item['taxes'] = {
                'code': tax['name'],
                'percent': tax['amount'],
                'type': 'sales'
            }

        item['enableTaxesIn'] = utils.true_or_false(item['enableTaxesIn'])
        item['hasPriceRounding'] = utils.true_or_false(item['hasPriceRounding'])
        item['mandatoryReferral'] = \
            utils.true_or_false(item['mandatoryReferral'])

        if item['image'] is None:
            del item['image']

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


def _get_taxes(id, source_db):
    """
    get the dispensary taxes settings for each dispensary_id
    """
    sql = ("SELECT DISTINCT dispensary_id, amount, name "
           "FROM taxes "
           "WHERE dispensary_id={0}").format(id)

    data = etl.fromdb(source_db, sql) 
    try:
        lookup_taxes = etl.select(data, lambda rec: rec.dispensary_id==id)
        return etl.dicts(lookup_taxes)
    except KeyError:
        return 0


def _member_type(type):
    """
    Convert memberType mapping to string format for G1
    """
    if type == 1:
        return 'MEDICAL'
    return 'RECREATIONAL'


if __name__ == '__main__':
    extract(sys.argv[1], True)

from __future__ import division, print_function, absolute_import

import os
import sys
import inspect
import MySQLdb
import petl as etl
import json

from collections import OrderedDict
from datetime import timedelta

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
        dispensary_details = utils.load_db_data(source_db, dispensary_id,
                                                'dispensary_details')

        pricing = utils.load_membership_prices(source_db, dispensary_id)

        return transform(dispensary_details, pricing, organization_id, debug,
                         source_db)
    finally:
        source_db.close()


def transform(dispensary_details, pricing, organization_id, debug, source_db):
    """
    Load the transformed data into the destination(s)
    """
    # source data table
    general_settings = utils.view_to_list(dispensary_details)
    pricing_detail = utils.view_to_list(pricing)

    dispensary_cut_data = ['id', 'dispensary_id', 'menu_show_tax',
                           'logo_file_name', 'inactivity_logout',
                           'calculate_even_totals',
                           'default_customer_license_type',
                           'require_customer_referrer',
                           'membership_fee_enabled',
                           'pp_enabled',
                           'pp_global_dollars_to_points',
                           'pp_global_points_to_dollars',
                           'pp_points_per_referral', 'allow_unpaid_visits',
                           'red_flags_enabled', 'mmjrevu_api_key']

    pricing_cut_data = ['id', 'price_half_gram', 'price_gram',
                        'price_two_gram', 'price_eigth', 'price_quarter',
                        'price_half', 'price_ounce']

    dispensary_settings_data = etl.cut(general_settings, dispensary_cut_data)
    pricing_data = etl.cut(pricing_detail, pricing_cut_data)

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
            'inactivity_logout': 'sessionTimeoutDuration',
            # Global -> Logo
            'logo_file_name': 'image',
            # <Location> -> Sales -> TAXES IN
            'menu_show_tax': 'enableTaxesIn',
            # <Location> -> Sales -> PRICE ROUNDING
            'calculate_even_totals': 'hasPriceRounding',
            'default_customer_license_type': 'memberType',
            # <Location> -> Members -> REFERRER REQUIRED
            'require_customer_referrer': 'mandatoryReferral',
            # Global -> Members -> Membership Level
            'membership_fee_enabled': 'membershipLevelsEnabled',
            'pp_global_dollars_to_points': 'dollarsPerPoint',
            'pp_global_points_to_dollars': 'pointsPerDollar',
            'pp_points_per_referral': 'referralPoints',
            # <Location> -> Members -> PAID VISITS
            'allow_unpaid_visits': 'paidVisitsEnabled',
            # <Location> -> Members -> MEDICAL MEMBERS
            'red_flags_enabled': 'hasLimits',
            # <Location> -> General -> STORE LOCATIONS
            'mmjrevu_api_key': 'apiKey'
        })
    )
    settings = []
    for item in etl.dicts(merged_settings):
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id']
        }

        # remove any item['keys'] tuples with None values
        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

        item['memberType'] = _member_type(item['memberType'])

        # sales.settings.taxes
        for tax in _get_taxes(item['dispensary_id'], source_db):
            item['taxes'] = {
                'code': tax['name'],
                'percent': tax['amount'],
                'type': 'sales'
            }

        for pricing in etl.dicts(pricing_data):
            item['weightPricing'] = {
                'name': 'Default',
                'defaultTier': True
            }
            item['weightPricing']['breakpoints'] = {
                'price_half_gram': pricing['price_half_gram'],
                'price_gram': pricing['price_gram'],
                'price_eighth': pricing['price_eigth'],
                'price_quarter': pricing['price_quarter'],
                'price_half': pricing['price_half'],
                'price_ounce': pricing['price_ounce'],
            }
        
        item['apiKey'] = item['apiKey']

        # inventory.categories
        for category in _get_categories(item['dispensary_id'], source_db):
            item['categories'] = {
                'name': category['name'],
                'tax_free': False
            }

        item['enableTaxesIn'] = utils.true_or_false(item['enableTaxesIn'])
        item['hasPriceRounding'] = utils.true_or_false(item['hasPriceRounding'])
        item['mandatoryReferral'] = \
            utils.true_or_false(item['mandatoryReferral'])
        item['paidVisitsEnabled'] = \
            utils.true_or_false(item['paidVisitsEnabled'])

        if item['pp_enabled']:
            item['membershipLevel'] = {
                'membershipLevelsEnabled': \
                    utils.true_or_false(item['membershipLevelsEnabled']),
                'levelName': 'Unnamed',
                'dollarsPerPoint': item['dollarsPerPoint'],
                'pointsPerDollar': item['pointsPerDollar'],
                'referralPoints': item['referralPoints']
            }

        # monthly purchase limit is two week limit x2
        if item['hasLimits'] == 1:
            for limits in _medical_limits(item['dispensary_id'], source_db):
                item['memberLimits'] = {
                    'hasLimits': True,
                    'dailyPurchaseLimit': limits['daily_purchase_limit'],
                    'visitPurchaseLimit': limits['visit_purchase_limit'],
                    'dailyVisitLimit': limits['daily_visit_limit'],
                    'monthlyPurchaseLimit': \
                        int(limits['two_week_purchase_limit'] * 2)
                }


        if item['image'] is None or item['apiKey'] is None:
            del item['image']
            del item['apiKey']


        # delete fk's
        del item['id']
        del item['dispensary_id']
        del item['membershipLevelsEnabled']

        # set up final structure for API
        settings.append(item)

    if debug:
        result = json.dumps(settings, sort_keys=True, indent=4,
                            default=utils.json_serial)
        print(result)

    return settings


def _medical_limits(id, source_db):
    """
    get the member limits
    """
    sql = ("SELECT dispensary_id, daily_purchase_limit, visit_purchase_limit, "
           "daily_visit_limit, two_week_purchase_limit "
           "FROM red_flags "
           "WHERE dispensary_id={0}").format(id)

    data = etl.fromdb(source_db, sql) 
    limits = etl.select(data, lambda rec: rec.dispensary_id==id)
    return etl.dicts(limits)


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


def _get_categories(id, source_db):
    """
    get the categories for each dispensary_id
    """
    sql = ("SELECT dispensary_id, name "
           "FROM categories "
           "WHERE dispensary_id={0}").format(id)

    data = etl.fromdb(source_db, sql) 
    try:
        categories = etl.select(data, lambda rec: rec.dispensary_id==id)
        return etl.dicts(categories)
    except KeyError:
        return None


def _member_type(type):
    """
    Convert memberType mapping to string format for G1
    """
    if type == 1:
        return 'MEDICAL'
    return 'RECREATIONAL'


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2], True)

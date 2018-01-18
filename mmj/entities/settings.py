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
    source_db = MySQLdb.connect(host="localhost",
                                user="root",
                                passwd="c0l3m4N",
                                db="mmjmenu_development")
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
            # Global -> Members -> Membership Level
            'membership_fee_enabled': 'membershipLevelsEnabled',
            'pp_global_dollars_to_points': 'dollarsPerPoint',
            'pp_global_points_to_dollars': 'pointsPerDollar',
            'pp_points_per_referral': 'referralPoints',
            
            # <Location> -> Sales -> TAXES IN
            'menu_show_tax': 'enableTaxesIn',
            # <Location> -> Sales -> PRICE ROUNDING
            'calculate_even_totals': 'hasPriceRounding',
            # <Location> -> Members -> REFERRER REQUIRED
            'require_customer_referrer': 'mandatoryReferral',
            # <Location> -> Members -> PAID VISITS
            'allow_unpaid_visits': 'paidVisitsEnabled',
            # <Location> -> Members -> MEDICAL MEMBERS
            'red_flags_enabled': 'hasLimits',
            # <Location> -> General -> STORE LOCATIONS
            'mmjrevu_api_key': 'apiKey'
        })
    )
    settings = {}
    for item in etl.dicts(merged_settings):
        item['keys'] = {
            'dispensary_id': item['dispensary_id'],
            'id': item['id']
        }

        # remove any item['keys'] tuples with None values
        for key in item['keys'].keys():
            if not item['keys'][key]:
                del item['keys'][key]

        """
        Member settings nested - crm.member.settings
        """
        item['crm.member.settings'] = {}
        if item['pp_enabled']:
            item['crm.member.settings']['membershipLevel'] = {
                'membershipLevelsEnabled': \
                    utils.true_or_false(item['membershipLevelsEnabled']),
                'levelName': 'Unnamed',
                'dollarsPerPoint': item['dollarsPerPoint'],
                'pointsPerDollar': item['pointsPerDollar'],
                'referralPoints': item['referralPoints']
            }

        """
        Location ettings nested. 
        """
        if item['apiKey']:
            item['location_specific'] = {
                'apiKey': item['apiKey']
            }
        else:
            item['location_specific'] = {}
        item['location_specific']['members'] = {
            'paidVisitsEnabled': utils.true_or_false(item['paidVisitsEnabled']),
            'mandatoryReferral': utils.true_or_false(item['mandatoryReferral'])
        }
        item['location_specific']['sales'] = {
            'enableTaxesIn': utils.true_or_false(item['enableTaxesIn']),
            'hasPriceRounding': utils.true_or_false(item['hasPriceRounding'])
        }


        # sales.settings.taxes
        item['sales.settings'] = {}
        for tax in _get_taxes(item['dispensary_id'], source_db):
            item['sales.settings']['taxes'] = {
                'code': tax['name'],
                'percent': tax['amount'],
                'type': 'sales'
            }

        for pricing in etl.dicts(pricing_data):
            item['location_specific']['inventory'] = {}
            item['location_specific']['inventory']['weightPricing'] = {
                'name': 'Default',
                'defaultTier': True
            }
            item['location_specific']['inventory']['weightPricing']['breakpoints'] = {
                'price_half_gram': pricing['price_half_gram'],
                'price_gram': pricing['price_gram'],
                'price_two_gram': pricing['price_two_gram'],
                'price_eighth': pricing['price_eigth'],
                'price_quarter': pricing['price_quarter'],
                'price_half': pricing['price_half'],
                'price_ounce': pricing['price_ounce'],
            }

        # monthly purchase limit is two week limit x2
        if item['hasLimits'] == 1:
            for limits in _medical_limits(item['dispensary_id'], source_db):
                item['location_specific']['members']['medicalLimits'] = {
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
        del item['enableTaxesIn']
        del item['hasLimits']
        del item['hasPriceRounding']
        del item['dollarsPerPoint']
        del item['mandatoryReferral']
        del item['paidVisitsEnabled']
        del item['pointsPerDollar']
        del item['pp_enabled']
        del item['referralPoints']

        # set up final structure for API
        settings.update(item)

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


if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2], True)

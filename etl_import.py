from utilities import utils
import sys
import json

import employees
import members
import menu_items
import vendors
import physicians

import time

def extract(organization_id):
    employees_extract = employees.extract(organization_id, False)
    members_extract = members.extract(organization_id, False)
    menu_items_extract = menu_items.extract(organization_id, False)
    vendors_extract = vendors.extract(organization_id, False)
    physicians_extract = physicians.extract(organization_id, False)

    # do some magic to pickle all of the extracts into
    # a single json structure that can be persisted into
    # the imports table

    payload = {
        'organizationId': organization_id,
        'employees': employees_extract,
        'members': members_extract,
        'products': menu_items_extract,
        'vendors': vendors_extract,
        'physicians': physicians_extract,
        'validated': False,
        'imported': False,
        'extractedDate': int(time.time()),
        'summary': {
            'members': {
                'validated': 0,
                'errors': []
            },
            'vendors': {
                'validated': 0,
                'errors': []
            },
            'products': {
                'validated': 0,
                'errors': []
            },
            'physicians': {
                'validated': 0,
                'errors': []
            },
            'employees': {
                'validated': 0,
                'errors': []
            }
        }
    }
    result = json.dumps(payload, sort_keys=True,
                        indent=4, default=utils.json_serial)
    #print(result)

    with open('mmj-{0}.json'.format(organization_id), 'w') as outfile:
        outfile.write(result)

    #connect to the POS db and insert the tranformed payload
    utils.mongo_connect_and_insert(payload)

if __name__ == '__main__':
    extract(sys.argv[1])

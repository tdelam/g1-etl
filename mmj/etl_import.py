import json
import sys
import time

from utilities import utils
from entities import employees, members, menu_items, physicians, vendors

def extract(dispensary_id, organization_id):
    employees_extract = employees.extract(dispensary_id, organization_id, False)
    members_extract = members.extract(dispensary_id, organization_id, False)
    menu_items_extract = menu_items.extract(dispensary_id, organization_id, False)
    vendors_extract = vendors.extract(dispensary_id, organization_id, False)
    physicians_extract = physicians.extract(dispensary_id, organization_id, False)

    # do some magic to pickle all of the extracts into
    # a single json structure that can be persisted into
    # the imports table

    payload = {
        'organizationId': str(organization_id),
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

    #connect to the G1 POS db and insert the tranformed payload
    utils.mongo_connect_and_insert(payload)

    # the rest endpoint needs a formatted result here indicating success of the
    # extraction process
    return result;

if __name__ == '__main__':
    extract(sys.argv[1], sys.argv[2])

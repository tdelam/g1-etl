from utilities import utils
import sys
import json

import employees
import members
import menu_items
import vendors
import physicians

def extract(organization_id):
    employees_extract = employees.extract(organization_id)
    members_extract = members.extract(organization_id)
    menu_items_extract = menu_items.extract(organization_id)
    vendors_extract = vendors.extract(organization_id)
    physicians_extract = physicians.extract(organization_id)

    # do some magic to pickle all of the extracts into
    # a single json structure that can be persisted into
    # the imports table
    payload = {
        'employees': employees_extract,
        'members': members_extract,
        'products': menu_items_extract,
        'vendors': vendors_extract,
        'physicians': physicians_extract
    }
    result = json.dumps(payload, sort_keys=True, indent=4, default=utils.json_serial)
    print(result)

    with open('mmj-{0}.json'.format(organization_id), 'w') as outfile:
        json.dump(result, outfile, sort_keys=True,
                  indent=4, default=utils.json_serial)

if __name__ == '__main__':
    extract(sys.argv[1])
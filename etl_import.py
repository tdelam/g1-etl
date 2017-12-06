from utilities import utils
import sys
import json

import employees
import members
import menu_items
import vendors
import physicians

def extract(organization_id):
    employees_extract = employees.extract(organization_id, False)
    members_extract = members.extract(organization_id, False)
    menu_items_extract = menu_items.extract(organization_id, False)
    vendors_extract = vendors.extract(organization_id, False)
    physicians_extract = physicians.extract(organization_id, False)

    payload = {
        'employees': employees_extract,
        'members': members_extract,
        'products': menu_items_extract,
        'vendors': vendors_extract,
        'physicians': physicians_extract
    }
    result = json.dumps(payload, sort_keys=True,
                        indent=4, default=utils.json_serial)
    #print(result)

    with open('mmj-{0}.json'.format(organization_id), 'w') as outfile:
        outfile.write(result)

if __name__ == '__main__':
    extract(sys.argv[1])

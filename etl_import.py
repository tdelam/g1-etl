import sys
import json
from utilities import utils
import employees
import members
import menu_items
import vendors
import physicians

def json_serial(obj):
    """JSON serializer for objects not serializable by default json code"""
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    raise TypeError("Type %s not serializable" % type(obj))

def extract(organization_id):
    employees_extract = employees.extract(organization_id)
    members_extract = members.extract(organization_id)
    menu_items_extract = menu_items.extract(organization_id)
    vendors_extract = vendors.extract(organization_id)
    employees_extract = employees.extract(organization_id)

    # do some magic to pickle all of the extracts into
    # a single json structure that can be persisted into
    # the imports table
    payload = {
        'employees': employees_extract,
        'members': members_extract,
        'products': menu_items_extract,
        'vendors': vendors_extract,
        'employees': employees_extract
    }
    result = json.dumps(payload, sort_keys=True, indent=4, default=json_serial)
    print(result)


if __name__ == '__main__':
    extract(sys.argv[1])

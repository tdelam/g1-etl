from __future__ import division, print_function, absolute_import

from collections import OrderedDict

import petl

def transform():
    table = [
        ["id", "name", "category_id", "state"],
        [1, "Horse", 5, "Stable"],
        [2, "Cow", 5, "Stable"],
        [3, "Pig", 2, "Unstable"],
        [4, "Chicken", 3, "Advancement"]
    ]

    data_dict = {
        'Unstable': 's89348932j',
        'Stable': 's87ns2932j'
    }

    mappings = OrderedDict()
    mappings["id"] = "id"
    mappings["name"] = "name"
    mappings["category_id"] = "state", lambda value: data_dict[value]

    table2 = petl.fieldmap(table, mappings)
    print(table2)

if __name__ == '__main__':
    transform()
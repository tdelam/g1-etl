import jwt
import requests

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import load_pem_public_key

SHARED_KEY = {
    'key': '8rDLYiMzi5GqtS8Ntu7kH21bWYrHAe54'
}

def post_vendors(token):
    url = 'http://localhost:3004/api/mmjetl/load/vendors'
    headers = {'Authorization': 'Bearer {0}'.format(token)}
    vendor = {
        'website': '',
        'id': 29368245,
        'entityType': 'vendor',
        'licenceNumber': '84298845BD',
        'phone': '8165546978',
        'address': {
            'city': 'Modesto',
            'zip': '51218',
            'country': '',
            'line2': '',
            'line1': '89742 E. Shore Ave',
            'state': 'CA'
        },
        'name': 'A Green Thmb',
        'accountStatus': 'ACTIVE',
    }
    r = requests.post(url, data=vendor, headers=headers);
    return r


def encode():
    encode = jwt.encode(SHARED_KEY, load_private_key(), algorithm='RS256')
    return encode


def decode():
    return jwt.decode(encode(),
                      load_public_key(), algorithms=['RS256'])


def load_private_key():
    with open('keys/private_key.pem', 'rb') as pem_in:
        pemlines_in = pem_in.read()
    priv_key = load_pem_private_key(pemlines_in, None, default_backend())
    return priv_key


def load_public_key():
    with open('keys/public_key.pem', 'rb') as pem_out:
        pemlines_out = pem_out.read()
    pub_key = load_pem_public_key(pemlines_out, default_backend())
    return pub_key


if __name__ == '__main__':
    # print(encode())
    # print(decode())
    post_vendors(encode())
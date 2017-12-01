from __future__ import absolute_import # needed for jwt

import jwt

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import load_pem_public_key

SHARED_KEY = {
    'key': '8rDLYiMzi5GqtS8Ntu7kH21bWYrHAe54'
}


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


def jwt_encode():
    encode = jwt.encode(SHARED_KEY, load_private_key(), algorithm='RS256')
    return encode


def jwt_decode():
    return jwt.decode(encode(),
                      load_public_key(), algorithms=['RS256'])

import jwt

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives.serialization import load_pem_private_key
from cryptography.hazmat.primitives.serialization import load_pem_public_key


def encode():
    return jwt.encode({'some': 'payload'},
                      load_private_key(), algorithm='RS256')


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
    encode()
    decode()
    import pdb; pdb.set_trace()
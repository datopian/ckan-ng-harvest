import base64
import hashlib
import json
import os


# we need a way to save as file using an unique identifier
# TODO check if base64 is the best idea
def encode_identifier(identifier):
    bytes_identifier = identifier.encode('utf-8')
    encoded = base64.b64encode(bytes_identifier)
    encoded_identifier = str(encoded, 'utf-8')

    return encoded_identifier


def decode_identifier(encoded_identifier):
    decoded_bytes = base64.b64decode(encoded_identifier)
    decoded_str = str(decoded_bytes, 'utf-8')

    return decoded_str


def hash_dataset(dataset):
        # hash the dataset.
        dmp_dataset = json.dumps(dataset, sort_keys=True)
        str_to_hash = dmp_dataset.encode('utf-8')
        return hashlib.sha256(str_to_hash).hexdigest()


def set_extra(ckan_dataset, key, value):
    found = False
    for extra in ckan_dataset['extras']:
        if extra['key'] == key:
            extra['value'] = value
            found = True
    if not found:
        ckan_dataset['extras'].append({'key': key, 'value': value})
    return ckan_dataset
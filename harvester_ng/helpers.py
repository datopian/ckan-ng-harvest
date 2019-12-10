import base64
import hashlib
import json
import os
import sys


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
        """ hash one dataset (a dict)."""
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


def read_ckan_api_key_from_db(sql_alchemy_url, user='admin'):
    """ Read the CKAN API KEY from database 
        Returns atuple:
            API_KEY or None on error,
            Error or None if OK """

    import sqlalchemy as db
    # string connection to CKAN psql, like: postgresql://ckan:123456@db/ckan
    # readed from CKAN secrets in CKAN CLOUD DOCKER or defined locally
    try:
        engine = db.create_engine(sql_alchemy_url)
    except Exception as e:
        error = f'Failed to connect with CKAN DB {sql_alchemy_url}: {e}'
        return None, error
    
    conn = engine.connect()
    result = conn.execute("select apikey from public.user where name='{user}'")
    try:
        row = result.fetchone()
    except Exception as e:
        error = f'Unable to read API KEY from database {sql_alchemy_url}: {e}'
        return None, error

    if row is None:
        error = f'There is no API KEY for the user: "{user}"'
        return None, error

    result.close()
    return row['apikey'], None
    
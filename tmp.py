from libs.data_gov_api import CKANPortalAPI
from libs.data_gov_api import logger
from libs.data_json import DataJSON
from random import randint
import logging
logger.setLevel(logging.DEBUG)
import csv


"""

CKAN_API_KEY = '79744bbe-f27b-46c8-a1e0-8f7264746c86'  # put your own local API key
cpa = CKANPortalAPI(base_url='http://ckan:5000', api_key=CKAN_API_KEY)

res = cpa.delete_package(ckan_package_ir_or_name='62bd2967-e74d-4bd8-8a80-138b2c8056d7')
res = cpa.create_harvest_source(title='Energy JSON test {}'.format(randint(1, 999999)),
                                        url='http://www.energy.gov/data.json',
                                        owner_org_id='my-local-test-organization-v2',
                                        notes='Some tests about local harvesting sources creation',
                                        frequency='WEEKLY')
print(res)
"""

""" already imported
# import all data.gov harvest sources
cpa.import_harvest_sources(catalog_url='https://catalog.data.gov',
                           owner_org_id='my-local-test-organization-v2'
                           )
"""
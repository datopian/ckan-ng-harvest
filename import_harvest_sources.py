from libs.data_gov_api import CKANPortalAPI
from libs.data_gov_api import logger
import logging
logger.setLevel(logging.DEBUG)


CKAN_API_KEY = '2de6add4-bd1c-4f66-9e2b-37f4bc3ddd0f'  # put your own local API key
cpa = CKANPortalAPI(base_url='http://ckan:5000', api_key=CKAN_API_KEY)

# import all data.gov harvest sources
cpa.import_harvest_sources(catalog_url='https://catalog.data.gov')

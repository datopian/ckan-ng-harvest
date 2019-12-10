import os
import pytest
from harvesters.logs import logger
from harvester_adapters.ckan.api import CKANPortalAPI
from harvester_ng import helpers
from dotenv import load_dotenv


# read the .env local file
load_dotenv()


@pytest.mark.vcr()
def test_update_dataset():
    """ harvest a dataset and check if the second time is updated, not duplicated. """

    # we need 
    catalog_url = os.environ.get('CKAN_BASE_URL', None)
    catalog_api_key = os.environ.get('CKAN_API_KEY', None)

    api_key_from_db = catalog_api_key == 'READ_FROM_DB'
    if api_key_from_db:
        sql_alchemy_url = os.environ.get('SQLALCHEMY_URL', None)
        api_key, error = helpers.read_ckan_api_key_from_db(sql_alchemy_url)
        if error is not None:
            raise Exception(error)

        os.environ['CKAN_API_KEY'] = api_key
        catalog_api_key = api_key
        logger.info('Read API KEY from database: {} ({})'.format(api_key, sql_alchemy_url))
    
    # use (and save with VCR) a source with just 4 datasets
    harvest_from = 'https://www.onhir.gov/data.json'

    cpa = CKANPortalAPI(base_url=catalog_url, api_key=catalog_api_key)

    organization = {
            'name': 'test-organization',
            'title': 'Test Organization',
            'state': 'active'
            }
    res = cpa.create_organization(organization=organization)

    harvest_source = cpa.create_harvest_source(title="Test harvest source",
                                               url=harvest_from,
                                               owner_org_id='test-organization',
                                               source_type='datajson',
                                               notes='Test harvest source',
                                               frequency='WEEKLY')

    


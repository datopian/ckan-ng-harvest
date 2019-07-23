import unittest
from libs.data_gov_api import CKANPortalAPI
base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'
import random
from slugify import slugify


class CKANPortalAPITestClass(unittest.TestCase):
    """ test a real CKAN API.
        #TODO test a local CKAN instance with real resource will be expensive but real test
        """
    """

    def test_load_from_url(self):
        cpa = CKANPortalAPI()
        harvest_source_id = '8d4de31c-979c-4b50-be6b-ea3c72453ff6'  # Dep Energy US Gov
        resources = 0

        page = 0
        for packages in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
            page += 1
            print(f'API packages search page {page}')
            self.assertGreater(cpa.total_packages, 0)  # has resources in the first page
            break  # do not need more
    """

    def test_create_package(self):
        # needs a local CKAN instance with an organization id = 'my-local-test-organization-v2'
        #TODO improve this test to check requirements

        CKAN_API_KEY = '79744bbe-f27b-46c8-a1e0-8f7264746c86'  # put your own local API key
        cpa = CKANPortalAPI(base_url='http://ckan:5000', api_key=CKAN_API_KEY)

        # error if duplicated
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        package = {'name': dataset_name, 'title': dataset_title, 'owner_org': 'my-local-test-organization-v2'}
        res = cpa.create_package(package=package)
        print(res)
        self.assertTrue(res['success'])

        print(res)
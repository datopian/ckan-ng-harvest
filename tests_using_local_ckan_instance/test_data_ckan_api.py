import unittest
from libs.data_gov_api import CKANPortalAPI
import random
from slugify import slugify
import json
# put you settings in the local_settings hidden-to-github file
from tests_using_local_ckan_instance.settings import (HARVEST_SOURCE_ID,
                                                      CKAN_API_KEY,
                                                      CKAN_BASE_URL,
                                                      CKAN_ORG_ID,
                                                      CKAN_VALID_USER_ID
                                                      )


class CKANPortalAPITestClass(unittest.TestCase):
    """ test a real CKAN API.
        #TODO test a local CKAN instance with real resource will be expensive but real test
        """

    def test_load_from_url(self):
        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL)
        resources = 0

        page = 0
        for packages in cpa.search_harvest_packages(harvest_source_id=HARVEST_SOURCE_ID):
            page += 1
            print(f'API packages search page {page}')
            self.assertGreater(cpa.total_packages, 0)  # has resources in the first page
            break  # do not need more

    def test_create_package(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        # error if duplicated
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        package = {'name': dataset_name, 'title': dataset_title, 'owner_org': 'my-local-test-organization-v2'}
        res = cpa.create_package(ckan_package=package)
        print(res)
        self.assertTrue(res['success'])

    def test_create_package_with_tags(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        # error if duplicated
        dataset_title = 'Dataset number {}'.format(random.randint(1, 999999))
        dataset_name = slugify(dataset_title)
        tags = [{'name': 'tag001'}, {'name': 'tag002'}]

        package = {'name': dataset_name,
                   'title': dataset_title, 'owner_org': 'my-local-test-organization-v2',
                   'tags': tags}
        res = cpa.create_package(ckan_package=package)
        print(res)
        self.assertTrue(res['success'])

    def test_create_harvest_source(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
        res = cpa.create_harvest_source(title='Energy JSON test {}'.format(random.randint(1, 999999)),
                                        url='http://www.energy.gov/data.json',
                                        owner_org_id=CKAN_ORG_ID,
                                        notes='Some tests about local harvesting sources creation',
                                        frequency='WEEKLY')
        print(res)
        self.assertTrue(res['success'])

        # delete it
        res2 = cpa.delete_package(ckan_package_ir_or_name=res['result']['name'])
        self.assertTrue(res['success'])

    def test_get_admins(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        res = cpa.get_admin_users(organization_id=CKAN_ORG_ID)
        print(res)
        self.assertTrue(res['success'])

    def test_get_user_info(self):

        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        res = cpa.get_user_info(user_id=CKAN_VALID_USER_ID)
        print(res)
        self.assertTrue(res['success'])

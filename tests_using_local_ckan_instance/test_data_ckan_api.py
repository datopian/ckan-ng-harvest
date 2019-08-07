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
        title = 'Energy JSON test {}'.format(random.randint(1, 999999))
        url = 'http://www.energy.gov/data-{}.json'.format(random.randint(1, 999999))
        res = cpa.create_harvest_source(title=title,
                                        url=url,
                                        owner_org_id=CKAN_ORG_ID,
                                        source_type='datajson',
                                        notes='Some tests about local harvesting sources creation',
                                        frequency='WEEKLY')

        self.assertTrue(res['success'])
        dataset_name = res['result']['name']
        dataset_id = res['result']['id']

        # read it
        res = cpa.show_package(ckan_package_ir_or_name=dataset_id)
        self.assertTrue(res['success'])
        dataset = res['result']
        self.assertEqual(dataset['url'], url)
        self.assertEqual(dataset['title'], title)
        self.assertEqual(dataset['type'], 'harvest')
        self.assertEqual(dataset['source_type'], 'datajson')
        print(dataset)

        # search for it
        results = cpa.search_harvest_packages(rows=1000,
                                               harvest_type='harvest',  # harvest for harvest sources
                                               # source_type='datajson'
                                               )

        created_ok = False
        for datasets in results:
            for dataset in datasets:
                print('FOUND: {}'.format(dataset['name']))
                if dataset['name'] == dataset_name:
                    created_ok = True

        # ---------------------------------------------------------
        # FIXME The dataset was created OK but the search didn't find it.
        # Nopt sure what's happening
        assert created_ok == True
        # ---------------------------------------------------------

        # delete it
        res2 = cpa.delete_package(ckan_package_ir_or_name=dataset_name)
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

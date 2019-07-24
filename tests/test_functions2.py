"""
Tests all functions used in flow file
"""
import unittest
import config
from functions import get_data_json_from_url, save_as_data_packages
from functions2 import compare_resources

base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'


class Functions2TestClass(unittest.TestCase):

    def test_compare_resources(self):
        config.SOURCE_NAME = 'usada-test'
        url = f'{base_url}/usda.gov.data.json'
        config.SOURCE_URL = url
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1
            save_as_data_packages(dataset)

        self.assertEqual(total, 1580)

        # compare with fake results
        fake_rows = [
            # extras do not exist
            {'id': '0001',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'NO-extras': [{'key': 'id', 'value': '000'}]},
            # key "identifier" do not exist inside extras
            {'id': '0002',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'id', 'value': '000'}]},
            # must be marked for update
            {'id': '0003',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'usda-ocio-15-01'}]},
            # NOT MODIFIED (by date)
            {'id': '0004',
             'metadata_modified': '2014-10-03T14:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'USDA-DM-003'}]},
            # NEW unknown identifier. I need to delete if is not in data.json
            {'id': '0005',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'New unexpected identifier'}]},
        ]

        for row in compare_resources(rows=fake_rows):
            # I expect first resoults

            cr = row['comparison_results']
            ckan_id = cr.get('ckan_id', None)

            if ckan_id == '0001':
                self.assertEqual(cr['action'], 'error')
                self.assertEqual(cr['reason'], 'The CKAN dataset does not '
                                              'have the "extras" property')
            elif ckan_id == '0002':
                self.assertEqual(cr['action'], 'error')
                self.assertEqual(cr['reason'], 'The CKAN dataset does not have an "identifier"')

            elif ckan_id == '0003':
                self.assertEqual(cr['action'], 'update')
                self.assertIsInstance(cr['new_data'], dict)

            elif ckan_id == '0004':
                self.assertEqual(cr['action'], 'ignore')
                self.assertIsNone(cr['new_data'])

            elif ckan_id == '0005':
                self.assertEqual(cr['action'], 'delete')










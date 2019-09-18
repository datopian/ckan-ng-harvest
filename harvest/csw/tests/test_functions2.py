"""
Tests all functions used in flow file
"""
from unittest import TestCase, mock
from functions import get_csw_from_url, save_as_data_packages
from functions2 import get_current_ckan_resources_from_api, compare_resources
from harvester import config
from tests.mock_csw import MockCatalogueServiceWeb
from harvester.logs import logger


class TestFunctions2(TestCase):

    def mocked_csw(url=None, timeout=30):
        return MockCatalogueServiceWeb(url=url)

    @mock.patch('harvester.csw.CatalogueServiceWeb', side_effect=mocked_csw)
    def test_compare_resources(self, mock_csw):
        config.SOURCE_NAME = 'some-csw'
        url = 'https://some-source.com/csw-records'
        config.SOURCE_URL = url
        total = 0

        config.LIMIT_DATASETS = 4
        for record in get_csw_from_url(url=url):
            self.assertIsInstance(record, dict)
            total += 1
            print(record['identifier'])
            save_as_data_packages(record)

        mock_csw.assert_called_once()

        # compare with fake results
        fake_rows = [
            # extras do not exist
            {'id': '0001',
             'NO-extras': [{'key': 'id', 'value': '000'}]},
            # key "identifier" do not exist inside extras
            {'id': '0002',
             'extras': [{'key': 'id', 'value': '000'}]},
            # must be marked for update
            {'id': '0003',
             'extras': [{'key': 'identifier', 'value': 'OTDS.082019.32616.1'}]},
            # must be marked for update
            {'id': '0004',
             'extras': [{'key': 'identifier', 'value': 'OTDS.082019.32616.2'}]},
            # NEW unknown identifier. I need to delete if is not in data.json
            {'id': '0005',
             'extras': [{'key': 'identifier', 'value': 'New unexpected identifier'}]},
        ]

        for row in compare_resources(rows=fake_rows):
            # I expect first resoults

            logger.info(f'processing row {row}')
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
                self.assertEqual(cr['action'], 'update')
                self.assertIsInstance(cr['new_data'], dict)

            elif ckan_id == '0005':
                self.assertEqual(cr['action'], 'delete')










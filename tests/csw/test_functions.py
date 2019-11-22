"""
Tests all functions used in flow file
"""
import json
from unittest import TestCase, mock
from harvester_ng.csw.functions import clean_duplicated_identifiers, get_csw_from_url
from tests.csw.mock_csw import MockCatalogueServiceWeb


class FunctionsTestClass(TestCase):

    def mocked_csw(url=None, timeout=30):
        return MockCatalogueServiceWeb(url=url)

    @mock.patch('harvesters.csw.harvester.CatalogueServiceWeb', side_effect=mocked_csw)
    def test_404_csw(self, mock_csw):
        url = 'https://some-source.com/404csw'
        with self.assertRaises(Exception) as context:
            for dataset in get_csw_from_url(url=url):
                pass
        mock_csw.assert_called_once()
        print(context.exception)
        self.assertTrue('Fail to connect' in str(context.exception))
        self.assertTrue('404 Client Error' in str(context.exception))

    @mock.patch('harvesters.csw.harvester.CatalogueServiceWeb', side_effect=mocked_csw)
    def test_csw_data(self, mock_csw):
        url = 'https://some-source.com/csw-records'

        total = 0
        for record in get_csw_from_url(url=url):
            total += 1
            print('Record: {}'.format(record.get('identifier', '')))

        # print(mock_csw.mock_calls)
        mock_csw.assert_called_once()
        self.assertEqual(total, 363)

"""
Tests all functions used in flow file
"""
import unittest
from functions import clean_duplicated_identifiers, get_data_json_from_url

base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'


class FunctionsTestClass(unittest.TestCase):

    def test_404_get_data_json(self):
        url = f'{base_url}/DO-NOT-EXISTS.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                print(dataset)
        self.assertTrue('HTTP error: 404' in str(context.exception))

    def test_bad_get_data_json(self):
        url = f'{base_url}/bad.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                print(dataset)
        self.assertTrue('Expecting property name enclosed in double quotes: line 3 column 5 (char 25)' in str(
            context.exception))

    def test_good_get_data_json(self):
        url = f'{base_url}/usda.gov.data.json'
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(total, 1580)

    def test_goodwitherrors_get_data_json(self):
        url = f'{base_url}/healthdata.gov.data.json'
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(total, 1762)

    def test_limit(self):
        url = f'{base_url}/healthdata.gov.data.json'
        total = 0
        import config
        config.LIMIT_DATASETS = 15
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(total, 15)

    def test_clean_duplicated_identifiers_bad_field(self):
        rows = [{'bad_field_identifier': 'ya/&54'}]

        with self.assertRaises(KeyError):
            for dataset in clean_duplicated_identifiers(rows):
                self.assertIsInstance(dataset, dict)

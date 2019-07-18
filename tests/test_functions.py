"""
Tests all functions used in flow file
"""
import unittest

import config
from functions import clean_duplicated_identifiers, get_data_json_from_url

base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'


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
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)

    def test_goodwitherrors_get_data_json(self):
        url = f'{base_url}/healthdata.gov.data.json'
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)

    def test_clean_duplicated_identifiers_bad_field(self):
        rows = [{'bad_field_identifier': 'ya/&54'}]

        with self.assertRaises(KeyError):
            for dataset in clean_duplicated_identifiers(rows):
                self.assertIsInstance(dataset, dict)

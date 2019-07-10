"""
Tests all functions used in flow file
"""
import unittest

from functions import (get_data_json_from_url, 
                            clean_duplicated_identifiers,
                            get_current_ckan_resources_from_api,
                            dbg_packages,
                            compare_resources
                            )


base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'

class FunctionsTestClass(unittest.TestCase):

    def test_404_get_data_json(self):
        url = f'{base_url}/DO-NOT-EXISTS.json'
        path = 'data/data1.json'
        with self.assertRaises(Exception):
            for dataset in get_data_json_from_url(url=url, name='Do-not-exists', data_json_path=path):
                print(dataset)
        
            
    def test_bad_get_data_json(self):
        url = f'{base_url}/bad.json'
        path = 'data/data2.json'
        with self.assertRaises(Exception):
            for dataset in get_data_json_from_url(url=url, name='Bad JSON', data_json_path=path):
                print(dataset)

    def test_empty_get_data_json(self):
        url = f'{base_url}/good-but-not-data.json'
        path = 'data/data3.json'
        with self.assertRaises(Exception):
            for dataset in get_data_json_from_url(url=url, name='Empty JSON', data_json_path=path):
                print(dataset)

    def test_good_get_data_json(self):
        url = f'{base_url}/usda.gov.data.json'
        path = 'data/data4.json'
        for dataset in get_data_json_from_url(url=url, name='Good data.json', data_json_path=path):
            self.assertIsInstance(dataset, dict)

    def test_goodwitherrors_get_data_json(self):
        url = f'{base_url}/healthdata.gov.data.json'
        path = 'data/data5.json'
        ret = get_data_json_from_url(url=url, name='Do-not-exists', data_json_path='data')
        for dataset in get_data_json_from_url(url=url, name='Good data.json', data_json_path=path):
            self.assertIsInstance(dataset, dict)

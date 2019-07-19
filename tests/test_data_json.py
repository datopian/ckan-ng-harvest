import unittest
from libs.data_json import DataJSON
from functions import get_data_json_from_url
base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'


class DataJSONTestClass(unittest.TestCase):

    def test_load_from_url(self):
        dj = DataJSON()

        ret, error = dj.download_data_json()
        self.assertFalse(ret)  # No URL

        dj.url = f'{base_url}/DO-NOT-EXISTS.json'
        ret, error = dj.download_data_json()
        self.assertFalse(ret)  # URL do not exists (404)

        dj.url = f'{base_url}/bad.json'
        ret, error = dj.download_data_json()
        self.assertTrue(ret)  # URL exists but it's a bad JSON, do not fails, it's downloadable (OK)

    def test_read_json(self):
        dj = DataJSON()

        dj.url = f'{base_url}/bad.json'
        ret, error = dj.download_data_json()

        ret, error = dj.load_data_json()
        self.assertFalse(ret)  # it's a bad JSON

        dj.url = f'{base_url}/good-but-not-data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        self.assertTrue(ret)  # it's a good JSON

    def test_validate_json1(self):

        dj = DataJSON()

        dj.url = f'{base_url}/good-but-not-data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()
        self.assertFalse(ret)  # no schema

    def test_validate_json2(self):
        # data.json without errors
        dj = DataJSON()

        dj.url = f'{base_url}/usda.gov.data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()

        self.assertTrue(ret)  # schema works without errors
        self.assertEqual(None, errors)

    def test_validate_json3(self):
        # data.json with some errors
        dj = DataJSON()

        dj.url = f'{base_url}/healthdata.gov.data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()

        self.assertFalse(ret)  # schema works but has errors
        self.assertEqual(1, len(errors))  # 1 schema errors

    def test_get_data_json_from_url(self):

        url = f'{base_url}/good-but-not-data.json'

        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(total, 1)

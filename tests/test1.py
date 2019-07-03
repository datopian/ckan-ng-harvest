
import unittest

from libs.data_json import DataJSON


base_url = 'https://avdata99.gitlab.io/andres-harvesting-experiments-v2'

class DataJSONTestClass(unittest.TestCase):

    def test_load_from_url(self):
        dj = DataJSON()
        
        ret, error = dj.download_data_json()
        self.assertEqual(ret, False)  # No URL
        
        dj.url = f'{base_url}/DO-NOT-EXISTS.json'
        ret, error = dj.download_data_json()
        self.assertEqual(ret, False)  # URL do not exists (404)

        dj.url = f'{base_url}/bad.json'
        ret, error = dj.download_data_json()
        self.assertEqual(ret, False)  # URL exists but it's a bad JSON
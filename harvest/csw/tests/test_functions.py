"""
Tests all functions used in flow file
"""
from unittest import TestCase
from functions import clean_duplicated_identifiers, get_csw_from_url


class FunctionsTestClass(TestCase):

    base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'

    def test_404_get_data_json(self):
        url = f'{self.base_url}/DO-NOT-EXISTS/'
        with self.assertRaises(Exception) as context:
            for dataset in get_csw_from_url(url=url):
                print(dataset)
        print(context.exception)
        self.assertTrue('Fail to connect' in str(context.exception))
        self.assertTrue('404 Client Error' in str(context.exception))

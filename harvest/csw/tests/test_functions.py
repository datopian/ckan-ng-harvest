"""
Tests all functions used in flow file
"""
from unittest import TestCase, mock
from functions import clean_duplicated_identifiers, get_csw_from_url
from munch import munchify  # dict to object


class FunctionsTestClass(TestCase):

    def mocked_csw(*args, **kwargs):

        class MockCatalogueServiceWeb:
            from_file = 'samples/csw_sample.json'
            errors = []
            def __init__(self, url, timeout=30):
                if url == 'https://some-source.com/DO-NOT-EXISTS.json':
                    raise Exception('Fail to connect. 404 Client Error')
                else:
                    d = json.load(self.from_file)
                    self = munchify(d)
                    self.errors = []

            def getrecords2(**kwa):
                return []

        return MockCatalogueServiceWeb(*args, **kwargs)

    @mock.patch('harvester.csw.CatalogueServiceWeb', side_effect=mocked_csw)
    def test_404_get_data_json(self, mock_csw):
        url = 'https://some-source.com/DO-NOT-EXISTS.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_csw_from_url(url=url):
                pass
        mock_csw.assert_called_once()
        print(context.exception)
        self.assertTrue('Fail to connect' in str(context.exception))
        self.assertTrue('404 Client Error' in str(context.exception))

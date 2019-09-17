"""
Tests all functions used in flow file
"""
from unittest import TestCase, mock
from functions import clean_duplicated_identifiers, get_data_json_from_url
from functions3 import build_validation_error_email, send_validation_error_email
from harvester.data_json import DataJSON

base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'


class FunctionsTestClass(TestCase):

    # datajson = DataJSON()
    # ret, info = datajson.download_data_json(timeout=90)
    # ret, info = datajson.load_data_json()
    # ret, info = datajson.validate_json()

    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, json_data, status_code):
                self.content = content
                self.status_code = status_code
        url = args[0]
        if url == 'https://some-source.com/DO-NOT-EXISTS.json':
            content = None
            status_code = 404
        elif url == 'https://some-source.com/BAD.json':
            content = '{"a": 1, LALALLA}'
            status_code = 200
        elif url == 'https://some-source.com/usda.gov.data.json':
            f = open('samples/usda.gov.data.json', 'r')
            content = f.read()
            f.close()
            status_code = 200
        # internal query for specs
        elif url == 'https://project-open-data.cio.gov/v1.1/schema/catalog.json':
            f = open('samples/schema1.1.json', 'r')
            content = f.read()
            f.close()
            status_code = 200
        elif url == 'https://some-source.com/healthdata.gov.data.json':
            f = open('samples/healthdata.gov.data.json', 'r')
            content = f.read()
            f.close()
            status_code = 200
        else:
            content = f'UNDEFINED URL {url}'
            status_code = 400

        return MockResponse(content, status_code)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_404_get_data_json(self, mock_req):

        url = 'https://some-source.com/DO-NOT-EXISTS.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                pass
        mock_req.assert_called_once()
        print(str(context.exception))
        self.assertTrue('Error getting data' in str(context.exception))

    @mock.patch("functions3.send_validation_error_email")
    @mock.patch("functions3.build_validation_error_email")
    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_bad_get_data_json(self,
                               mock_req,
                               build_validation_mock,
                               send_validation_mock):

        url = 'https://some-source.com/BAD.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                pass

        mock_req.assert_called_once()
        print(str(context.exception))
        self.assertTrue('Error validating JSON' in str(context.exception))

    @mock.patch("functions3.send_validation_error_email")
    @mock.patch("functions3.build_validation_error_email")
    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_good_get_data_json(self,
                                mock_req,
                                build_validation_mock,
                                send_validation_mock):

        url = 'https://some-source.com/usda.gov.data.json'
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(len(mock_req.call_args_list), 3)
        self.assertEqual(total, 1580)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_goodwitherrors_get_data_json(self, mock_req):

        url = 'https://some-source.com/healthdata.gov.data.json'
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        self.assertEqual(len(mock_req.call_args_list), 3)
        self.assertEqual(total, 1762)

    def test_limit(self):
        url = f'{base_url}/healthdata.gov.data.json'
        total = 0
        from harvester import config
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

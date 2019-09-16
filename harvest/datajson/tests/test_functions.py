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

    @mock.patch.object(DataJSON, 'download_data_json')
    def test_404_get_data_json(self, mock_dj_down):

        mock_dj_down.return_value = False, 'HTTP error: 404'

        url = 'https://some-source.com/DO-NOT-EXISTS.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                pass
        mock_dj_down.assert_called_once()
        print(str(context.exception))
        self.assertTrue('Error getting data' in str(context.exception))

    @mock.patch("functions3.send_validation_error_email")
    @mock.patch("functions3.build_validation_error_email")
    @mock.patch.object(DataJSON, 'raw_data_json')
    @mock.patch.object(DataJSON, 'download_data_json')
    def test_bad_get_data_json(self,
                               mock_download_json,
                               mock_raw_data_json,
                               build_validation_mock,
                               send_validation_mock):
        mock_download_json.return_value = True, None
        mock_raw_data_json.text = '{"a": 1, LALAL}'

        url = 'https://some-bad.json'
        with self.assertRaises(Exception) as context:
            for dataset in get_data_json_from_url(url=url):
                pass

        mock_download_json.assert_called_once()

        print(str(context.exception))
        self.assertTrue('Error validating JSON' in str(context.exception))

    @mock.patch("functions3.send_validation_error_email")
    @mock.patch("functions3.build_validation_error_email")
    @mock.patch.object(DataJSON, 'raw_data_json')
    @mock.patch.object(DataJSON, 'download_data_json')
    def test_good_get_data_json(self,
                                mock_download_json,
                                mock_raw_data_json,
                                build_validation_mock,
                                send_validation_mock):
        f = open('samples/usda.gov.data.json', 'r')
        # mock_raw_data_json.return_value = f.read()
        mock_raw_data_json.text = '{"a": 1}'
        f.close()
        mock_download_json.return_value = True, None
        url = 'https://some-source/usda.gov.data.json'
        total = 0
        for dataset in get_data_json_from_url(url=url):
            self.assertIsInstance(dataset, dict)
            total += 1

        mock_download_json.assert_called_once()
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

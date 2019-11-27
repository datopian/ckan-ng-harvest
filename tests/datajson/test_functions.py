"""
Tests all functions used in flow file
"""
import json
from unittest import TestCase, mock
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination
from harvester_ng.datajson.flows import clean_duplicated_identifiers


class FunctionsTestClass(TestCase):

    def setUp(self):
        self.destination = CKANHarvestDestination(catalog_url='http://not-in-use.com',
                                                  api_key='xxxx',
                                                  organization_id='xxxx',
                                                  harvest_source_id='xxxx')

    def mocked_requests_get(*args, **kwargs):
        class MockResponse:
            def __init__(self, content, status_code):
                self.content = content
                self.status_code = status_code

            def json(self):
                return json.loads(self.content)

            @property
            def json_data(self):
                return self.json()

        url = args[0]
        if url == 'https://some-source.com/DO-NOT-EXISTS.json':
            content = None
            status_code = 404
        elif url == 'https://some-source.com/BAD.json':
            content = '{"a": 1, LALALLA}'
            status_code = 200
        elif url == 'https://some-source.com/usda.gov.data.json':
            f = open('tests/datajson/samples/usda.gov.data.json', 'r')
            content = f.read()
            f.close()
            status_code = 200
        # internal query for specs
        elif url == 'https://project-open-data.cio.gov/v1.1/schema/catalog.json':
            f = open('tests/datajson/samples/schema1.1.json', 'r')
            content = f.read()
            f.close()
            status_code = 200
        elif url == 'https://some-source.com/healthdata.gov.data.json':
            f = open('tests/datajson/samples/healthdata.gov.data.json', 'r')
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
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        
        with self.assertRaises(Exception) as context:
            hdj.download()
            
        mock_req.assert_called_once()
        print(str(context.exception))
        self.assertTrue('Error getting data' in str(context.exception))

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_bad_get_data_json(self,
                               mock_req):

        url = 'https://some-source.com/BAD.json'
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        with self.assertRaises(Exception) as context:
            hdj.download()

        mock_req.assert_called_once()
        print(str(context.exception))
        self.assertTrue('ERROR parsing JSON' in str(context.exception))

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_good_get_data_json(self,
                                mock_req):

        url = 'https://some-source.com/usda.gov.data.json'
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        hdj.limit_datasets = 3
        hdj.download()

        self.assertEqual(len(mock_req.call_args_list), 1)
        # self.assertEqual(len(hdj.source_datasets), 1580)
        self.assertEqual(len(hdj.source_datasets), 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_goodwitherrors_get_data_json(self, mock_req):

        url = 'https://some-source.com/healthdata.gov.data.json'
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        hdj.limit_datasets = 3
        hdj.download()
        for dataset in hdj.source_datasets:
            self.assertIsInstance(dataset, dict)
            

        self.assertEqual(len(mock_req.call_args_list), 1)
        # self.assertEqual(len(hdj.source_datasets), 1762)
        self.assertEqual(len(hdj.source_datasets), 3)

    @mock.patch('requests.get', side_effect=mocked_requests_get)
    def test_limit(self, mock_req):
        from harvesters import config
        config.LIMIT_DATASETS = 15
        url = 'https://some-source.com/healthdata.gov.data.json'
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        hdj.limit_datasets = 3
        hdj.download()

        self.assertEqual(len(mock_req.call_args_list), 1)
        # self.assertEqual(len(hdj.source_datasets), 15)
        self.assertEqual(len(hdj.source_datasets), 3)

    def test_clean_duplicated_identifiers_bad_field(self):
        rows = [{'bad_field_identifier': 'ya/&54'}]

        with self.assertRaises(KeyError):
            for dataset in clean_duplicated_identifiers(rows):
                self.assertIsInstance(dataset, dict)

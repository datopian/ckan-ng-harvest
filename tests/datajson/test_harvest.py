"""
Tests all functions used in flow file
"""
import json
import shutil
from unittest import TestCase, mock
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination
from harvester_ng.datajson.flows import clean_duplicated_identifiers, compare_resources


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
    
    def test_compare_resources(self):

        # compare with fake results
        fake_rows = [
            # extras do not exist
            {'id': '0001',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'NO-extras': [{'key': 'id', 'value': '000'}]},
            # key "identifier" do not exist inside extras
            {'id': '0002',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'id', 'value': '000'}]},
            # must be marked for update
            {'id': '0003',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'usda-ocio-15-01'}]},
            # NOT MODIFIED (by date)
            {'id': '0004',
             'metadata_modified': '2014-10-03T14:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'USDA-DM-003'}]},
            # NEW unknown identifier. I need to delete if is not in data.json
            {'id': '0005',
             'metadata_modified': '2019-05-02T21:36:22.693792',
             'extras': [{'key': 'identifier', 'value': 'New unexpected identifier'}]},
        ]

        f = compare_resources(data_packages_path='tests/datajson/samples/compare_test')
        # this files will be deleted after process, use a copy (in a external folder)
        shutil.copyfile('tests/datajson/samples/data-json-0003.json',
                        'tests/datajson/samples/compare_test/data-json-dXNkYS1vY2lvLTE1LTAx.json')
        shutil.copyfile('tests/datajson/samples/data-json-0004.json',
                        'tests/datajson/samples/compare_test/data-json-VVNEQS1ETS0wMDM=.json')

        for row in f(rows=fake_rows):
            # I expect first resoults

            cr = row['comparison_results']
            ckan_id = cr.get('ckan_id', None)

            if ckan_id == '0001':
                self.assertEqual(cr['action'], 'error')
                self.assertEqual(cr['reason'], 'The CKAN dataset does not '
                                              'have the "extras" property')
            elif ckan_id == '0002':
                self.assertEqual(cr['action'], 'error')
                self.assertEqual(cr['reason'], 'The CKAN dataset does not have an "identifier"')

            elif ckan_id == '0003':
                self.assertEqual(cr['action'], 'update')
                self.assertIsInstance(cr['new_data'], dict)

            elif ckan_id == '0004':
                self.assertEqual(cr['action'], 'ignore')
                self.assertIsNone(cr['new_data'])

            elif ckan_id == '0005':
                self.assertEqual(cr['action'], 'delete')

"""
Tests all functions used in flow file
"""
import shutil
import unittest
from harvester_ng.datajson.flows import compare_resources
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination


base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'


class Functions2TestClass(unittest.TestCase):

    def setUp(self):
        self.destination = CKANHarvestDestination(catalog_url='http://not-in-use.com',
                                                  api_key='xxxx',
                                                  organization_id='xxxx',
                                                  harvest_source_id='xxxx')

    def test_compare_resources(self):

        """
        url = f'{base_url}/usda.gov.data.json'
        hdj = HarvestDataJSON(name='Test Name',
                              url=url,
                              destination=self.destination)
        res = hdj.download()
        hdj.save_download_results(flow_results=res)
        """

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










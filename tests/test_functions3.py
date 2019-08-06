"""
Tests all functions used in flow file
"""
import unittest
import config
from functions3 import assing_collection_pkg_id, write_results_to_ckan


class Functions3TestClass(unittest.TestCase):

    def test_assing_collection_pkg_id(self):

        r1 = {
            'id': 'r1-0001',  # CKAN ID
            'comparison_results': {
                'action': 'create',
                'new_data': {
                    'identifier': 'USDA-9000',  # data.json id
                    'isPartOf': 'USDA-8000',
                    }
                },
            }

        r2 = {
            'id': 'r2-0002',  # CKAN ID
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-8000',  # data.json id
                    }
                },
            }

        r3 = {
            'id': 'r3-0003',  # CKAN ID
            'comparison_results': {
                'action': 'create',
                'new_data': {
                    'identifier': 'USDA-7000',  # data.json id
                    'isPartOf': 'USDA-1000',  # not exists
                    }
                },
            }

        r4 = {
            'id': 'r4-0004',  # CKAN ID
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-6000',  # data.json id
                    'isPartOf': 'USDA-8000',
                    }
                },
            }

        rows = [r1, r2, r3, r4]
        for row in assing_collection_pkg_id(rows):
            datajson_dataset = row['comparison_results']['new_data']
            if row['id'] == 'r1-0001':  # this is part of r2-0002 dataset
                assert datajson_dataset['collection_pkg_id'] == 'r2-0002'
            elif row['id'] == 'r4-0004':  # this is part of r2-0002 dataset
                assert datajson_dataset['collection_pkg_id'] == 'r2-0002'
            elif row['id'] == 'r3-0003':  # this has a unknown father
                assert datajson_dataset['collection_pkg_id'] == ''
            elif row['id'] == 'r2-0002':  # this has no father
                assert 'collection_pkg_id' not in datajson_dataset
            else:
                assert "You never get here {}".format(row['id']) == False

        # same with a different order
        rows = [r4, r3, r2, r1]
        for row in assing_collection_pkg_id(rows):
            datajson_dataset = row['comparison_results']['new_data']
            if row['id'] == 'r1-0001':  # this is part of r2-0002 dataset
                assert datajson_dataset['collection_pkg_id'] == 'r2-0002'
            elif row['id'] == 'r4-0004':  # this is part of r2-0002 dataset
                assert datajson_dataset['collection_pkg_id'] == 'r2-0002'
            elif row['id'] == 'r3-0003':  # this has a unknown father
                assert datajson_dataset['collection_pkg_id'] == ''
            elif row['id'] == 'r2-0002':  # this has no father
                assert 'collection_pkg_id' not in datajson_dataset
            else:
                assert "You never get here {}".format(row['id']) == False


    def test_write_results_to_ckan(self):
        self.assertIsNone('DO THIS')
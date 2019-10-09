"""
Tests all functions used in flow file
"""
import unittest
from harvester import config
from harvester.data_gov_api import CKANPortalAPI
from functions3 import write_results_to_ckan
from harvester.logs import logger
import json
import sys
from pathlib import Path
FULL_BASE_PROJECT_PATH = str(Path().cwd().parent.parent.absolute())
print('IMPORT: ' + FULL_BASE_PROJECT_PATH)

sys.path.append(FULL_BASE_PROJECT_PATH)

from settings import (HARVEST_SOURCE_ID,
                       CKAN_API_KEY,
                       CKAN_BASE_URL,
                       CKAN_ORG_ID,
                       CKAN_VALID_USER_ID
                      )


class Functions3TestClass(unittest.TestCase):

    def test_write_results(self):
        config.CKAN_API_KEY = CKAN_API_KEY
        config.CKAN_CATALOG_URL = CKAN_BASE_URL
        config.CKAN_OWNER_ORG = CKAN_ORG_ID
        config.SOURCE_ID = HARVEST_SOURCE_ID
        config.SOURCE_NAME = 'Some harvest source'

        rows = json.load(open('samples/for-test-flow2-datasets-results.json', 'r'))
        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

        for row in write_results_to_ckan(rows):

            success = row['comparison_results']['action_results']['success']
            if not success:
                errors = row['comparison_results']['action_results']['errors']
                error = f'Fail to save dats {errors}'
                logger.error(error)
                raise Exception(error)
            # read the package
            name = row.get('name', row.get('id', None))
            if name is None:
                error = f'No dataset name at {row}'
                logger.error(error)
                raise Exception(error)
            package_show = cpa.show_package(ckan_package_id_or_name=name)
            ckan_dataset = package_show['result']
            extras = ckan_dataset.get('extras', None)
            assert type(extras) == list
            logger.info(f'writed package: {ckan_dataset}')

            guids = [extra['value'] for extra in extras if extra['key'] == 'guid']
            assert len(guids) == 1
            guid = guids[0]
            if guid == '47da09d2-46c4-48db-b503-819dd0fa84dd':
                assert [''] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-reference-system']
                """
                assert ['unique ID 971897198'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'guid']
                assert ['other'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-data-service-type']
                assert ['WEEKLY'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'frequency-of-update']
                assert ['some@email.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'contact-email']
                assert ['coup res'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'coupled-resource']
                assert ['2019-02-02'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-date']
                assert ['en'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-language']
                assert ['2010-12-01T12:00:00Z'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'dataset-reference-date']

                assert ["['CC-BY', 'http://licence.com']"] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence']
                assert ['http://licence.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence_url']

                assert ['some'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-file']
                assert ['some descr'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-description']
                assert ['some type'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'graphic-preview-type']

                assert ['teb1'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'temporal-extent-begin']
                assert ['tee1'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'temporal-extent-end']

                # rp = [{'name': 'GSA', 'roles': ['admin', 'admin2']}, {'name': 'NASA', 'roles': ['moon']}]
                rp = 'GSA (admin, admin2); NASA (moon)'
                assert [rp] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'responsible-party']

                coords = '[[[{xmin}, {ymin}], [{xmax}, {ymin}], [{xmax}, {ymax}], [{xmin}, {ymax}], [{xmin}, {ymin}]]]'.format(xmax=-61.9, ymax=-33.1, xmin=34.3, ymin=51.8)
                spatial = '{{"type": "Polygon", "coordinates": {coords}}}'.format(coords=coords)

                assert [spatial] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial']
                """
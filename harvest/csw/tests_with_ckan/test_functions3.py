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
            package = package_show['result']
            extras = package.get('extras', None)
            assert type(extras) == list
            logger.info(f'writed package: {package}')
            identifier = [extra['value'] for extra in extras if extra['key'] == 'identifier'][0]
            # for usmetadata schema: identifier = [extra['value'] for extra in extras if extra['key'] == 'unique_id'][0]
            if identifier == 'USDA-9000':  # is R1
                r1['id'] = row['id']
                r1['new_package'] = package
            elif identifier == 'USDA-8000':  # is R2
                assert r2['id'] == row['id']
                r2['new_package'] = package
            elif identifier == 'USDA-7000':  # is R3
                r3['id'] = row['id']
                r3['new_package'] = package
            elif identifier == 'USDA-6000':  # is R4
                assert r4['id'] == row['id']
                r4['new_package'] = package
            else:
                error = "BAD Identifier {} AT {}".format(identifier, row)
                logger.error(error)
                raise Exception(error)

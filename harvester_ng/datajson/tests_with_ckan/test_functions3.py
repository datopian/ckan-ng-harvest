"""
Tests all functions used in flow file
"""
import json
import unittest

from harvester_adapters.ckan.api import CKANPortalAPI
from harvester_ng.csw.functions3 import (assing_collection_pkg_id,
                                         write_results_to_ckan)
from harvesters import config
from harvesters.logs import logger
from settings import (CKAN_API_KEY, CKAN_BASE_URL, CKAN_ORG_ID,
                      HARVEST_SOURCE_ID)


class Functions3TestClass(unittest.TestCase):

    def test_assing_collection_pkg_id(self):
        config.CKAN_API_KEY = CKAN_API_KEY
        config.CKAN_CATALOG_URL = CKAN_BASE_URL
        config.CKAN_OWNER_ORG = CKAN_ORG_ID
        config.SOURCE_ID = HARVEST_SOURCE_ID
        config.SOURCE_NAME = 'Some harvest source'

        f = open('samples/test_datasets.json', 'r')
        sample_js = f.read().replace('CKAN_ORG_ID', CKAN_ORG_ID)
        f.close()
        js = json.loads(sample_js)

        r1 = js['r1']
        r2 = js['r2']
        r3 = js['r3']
        r4 = js['r4']

        # create the required datasets
        cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
        res = cpa.create_package(ckan_package=r2, on_duplicated='DELETE')
        r2['id'] = res['result']['id']
        res = cpa.create_package(ckan_package=r4, on_duplicated='DELETE')
        r4['id'] = res['result']['id']

        # delete r1 and r3
        params = [{'fq': f'+identifier:"USDA-7000"'},
                  {'fq': f'+identifier:"USDA-9000"'}]

        for param in params:
            for pkgs in cpa.search_packages(search_params=param):
                for pkg in pkgs:
                    cpa.delete_package(ckan_package_id_or_name=pkg['id'])

        rowss = [
            [r1, r2, r3, r4],
            [r4, r3, r2, r1]  # same with a different order
            ]

        for rows in rowss:
            rows_processed = []
            for row in write_results_to_ckan(rows):
                rows_processed.append(row)
                # check if OK
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

            for row in assing_collection_pkg_id(rows_processed):

                datajson_dataset = row['comparison_results']['new_data']
                # We already show at functions3
                # package_show = cpa.show_package(ckan_package_id_or_name=row['id'])
                # package = package_show['result']
                package = row
                logger.info(f'Assigned package: {package}')
                extras = package.get('extras', None)
                assert type(extras) == list

                cpi = [extra['value'] for extra in extras if extra['key'] == 'collection_package_id']
                if row['id'] == r1['id']:  # this is part of r2-0002 dataset
                    print(package)
                    print('----------------------')
                    print(row)
                    assert len(cpi) == 1
                    ckan_collection_package_id = cpi[0]
                    assert ckan_collection_package_id == r2['id']
                elif row['id'] == r4['id']:  # this is part of r2-0002 dataset
                    assert len(cpi) == 1
                    ckan_collection_package_id = cpi[0]
                    assert ckan_collection_package_id == r2['id']
                elif row['id'] == r3['id']:  # this has a unknown father
                    assert len(cpi) == 0
                elif row['id'] == r2['id']:  # this has no father
                    assert len(cpi) == 0
                else:
                    assert "You never get here {}".format(row['id']) == False

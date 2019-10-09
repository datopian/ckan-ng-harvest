"""
Tests all functions used in flow file
"""
import unittest
from harvester import config
from harvester.data_gov_api import CKANPortalAPI
from functions3 import write_results_to_ckan
from harvester.logs import logger

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

        r1 = {
            'comparison_results': {
                'action': 'create',
                'new_data': {
                    'identifier': 'USDA-9000',  # data.json id
                    'isPartOf': 'USDA-8000',
                    'title': 'R1 the first datajson',
                    'headers': {
                        "schema_version": "1.1",
                        "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                        "@id": "https://www2.ed.gov/data.json",
                        "@type": "dcat:Catalog",
                        "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                        "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                        }
                    }
                },
            }

        r2 = {
            'name': 'r2-second',
            'title': 'R2 the second',
            'owner_org': CKAN_ORG_ID,
            'resources': [],
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-8000',  # data.json id
                    'title': 'R2-second',
                    'headers': {
                        "schema_version": "1.1",
                        "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                        "@id": "https://www2.ed.gov/data.json",
                        "@type": "dcat:Catalog",
                        "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                        "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                        }
                    }
                },
            }

        r3 = {
            'owner_org': CKAN_ORG_ID,
            'comparison_results': {
                'action': 'create',
                'new_data': {
                    'identifier': 'USDA-7000',  # data.json id
                    'isPartOf': 'USDA-1000',  # not exists
                    'title': 'R3 the third',
                    'headers': {
                        "schema_version": "1.1",
                        "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                        "@id": "https://www2.ed.gov/data.json",
                        "@type": "dcat:Catalog",
                        "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                        "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                        }
                    }
                },
            }

        r4 = {
            'name': 'r4-fourth',
            'title': 'R4 the fourth',
            'owner_org': CKAN_ORG_ID,
            'resources': [],
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-6000',  # data.json id
                    'isPartOf': 'USDA-8000',
                    'title': 'R4-fourth',
                    'headers': {
                        "schema_version": "1.1",
                        "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
                        "@id": "https://www2.ed.gov/data.json",
                        "@type": "dcat:Catalog",
                        "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
                        "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
                        }
                    }
                },
            }

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
                # read the package
                package_show = cpa.show_package(ckan_package_id_or_name=row['id'])
                package = package_show['result']
                extras = package.get('extras', None)
                assert type(extras) == list
                logger.info(f'writed package: {package}')
                identifier = [extra['value'] for extra in extras if extra['key'] == 'identifier'][0]
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
                    assert "You never get here {}".format(row['id']) == False

"""
Tests all functions used in flow file
"""
import unittest
from harvester import config
from harvester.data_gov_api import CKANPortalAPI
from functions3 import assing_collection_pkg_id, write_results_to_ckan
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

    # just test this schema
    schema = 'usmetadata'

    def test_assing_collection_pkg_id(self):
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
                    'description': 'some notes',
                    'contactPoint': {
                        "hasEmail": "mailto:j1@data.com",
                        "@type": "vcard:Contact",
                        "fn": "Jhon One"
                        },
                    'programCode': '009:102',
                    'bureauCode': '003:01',
                    'publisher': {'name': 'Some publisher'},
                    'modified': '2019-02-02T21:36:22.693792',
                    'keyword': ['tag32', 'tag90'],
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
            'public_access_level': 'public',
            'owner_org': CKAN_ORG_ID,
            'unique_id': 'USDA-8000',
            'contact_name': 'Jhon Contact',
            'program_code': '001:900',
            'bureau_code': '002:80',
            'contact_email': 'jhon@contact.com',
            'publisher': 'Publisher 2',
            'notes': 'Some notes',
            'modified': '2019-05-02T21:36:22.693792',
            'tag_string': 'tag19,tag33',
            'resources': [],
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-8000',  # data.json id
                    'title': 'R2-second',
                    'contactPoint': {
                        "hasEmail": "mailto:j2@data.com",
                        "@type": "vcard:Contact",
                        "fn": "Jhon Two"
                        },
                    'programCode': '002:302',
                    'bureauCode': '008:88',
                    'publisher': {'name': 'Some publisher II'},
                    'modified': '2019-02-02T21:36:22.693792',
                    'keyword': ['tag31', 'tag91'],
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
                    'contactPoint': {
                        "hasEmail": "mailto:j3@data.com",
                        "@type": "vcard:Contact",
                        "fn": "Jhon Three"
                        },
                    'programCode': '002:303',
                    'bureauCode': '008:83',
                    'publisher': {'name': 'Some publisher III'},
                    'modified': '2019-03-02T21:36:22.693792',
                    'keyword': ['tag33', 'tag93'],
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
            'public_access_level': 'public',
            'unique_id': 'USDA-6000',
            'contact_name': 'Jhon Contact II',
            'program_code': '003:200',
            'bureau_code': '007:10',
            'contact_email': 'jhonII@contact.com',
            'publisher': 'Publisher',
            'notes': 'Some notes II',
            'modified': '2012-05-02T21:36:22.693792',
            'tag_string': 'tag24,tag39',
            'resources': [],
            'comparison_results': {
                'action': 'update',
                'new_data': {
                    'identifier': 'USDA-6000',  # data.json id
                    'isPartOf': 'USDA-8000',
                    'title': 'R4-fourth',
                    'contactPoint': {
                        "hasEmail": "mailto:j4@data.com",
                        "@type": "vcard:Contact",
                        "fn": "Jhon Four"
                        },
                    'programCode': '002:304',
                    'bureauCode': '008:84',
                    'publisher': {'name': 'Some publisher IV'},
                    'modified': '2019-04-02T21:36:22.693792',
                    'keyword': ['tag34', 'tag94'],
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
                # for default schema: identifier = [extra['value'] for extra in extras if extra['key'] == 'identifier'][0]
                identifier = [extra['value'] for extra in extras if extra['key'] == 'unique_id'][0]
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
                package_show = cpa.show_package(ckan_package_id_or_name=row['id'])
                package = package_show['result']
                logger.info(f'Assigned package: {package}')
                extras = package.get('extras', None)
                assert type(extras) == list

                if row['id'] == r1['id']:  # this is part of r2-0002 dataset
                    ckan_collection_package_id = [extra['value'] for extra in extras if extra['key'] == 'collection_package_id'][0]
                    assert ckan_collection_package_id == r2['id']
                elif row['id'] == r4['id']:  # this is part of r2-0002 dataset
                    ckan_collection_package_id = [extra['value'] for extra in extras if extra['key'] == 'collection_package_id'][0]
                    assert ckan_collection_package_id == r2['id']
                elif row['id'] == r3['id']:  # this has a unknown father
                    ckan_collection_package_id = [extra['value'] for extra in extras if extra['key'] == 'collection_package_id']
                    assert [] == ckan_collection_package_id
                elif row['id'] == r2['id']:  # this has no father
                    ckan_collection_package_id = [extra['value'] for extra in extras if extra['key'] == 'collection_package_id']
                    assert [] == ckan_collection_package_id
                else:
                    assert "You never get here {}".format(row['id']) == False

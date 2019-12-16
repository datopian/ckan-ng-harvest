import sys
import unittest

from subprocess import call, check_output, Popen, PIPE

from harvesters.logs import logger
from harvester_adapters.ckan.api import CKANPortalAPI
from settings import CKAN_BASE_URL, CKAN_API_KEY, CKAN_ORG_ID


class HarvestTestClass(unittest.TestCase):

  def test_harvest(self):
    NAME = 'rrb'
    URL = 'https://secure.rrb.gov/data.json'

    cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
    cpa.delete_all_harvest_sources()

    harvest_source = cpa.create_harvest_source(title=NAME,
                                                url=URL,
                                                owner_org_id=CKAN_ORG_ID,
                                                source_type='datajson',
                                                notes='Some tests about local harvesting sources creation',
                                                frequency='WEEKLY')

    HARVEST_SOURCE_ID = harvest_source['result']['id']

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format( #nosec
      NAME,
      URL,
      HARVEST_SOURCE_ID,
      CKAN_ORG_ID,
      CKAN_BASE_URL,
      CKAN_API_KEY)],
      shell=True,
      stdout=PIPE)

    result.communicate()

    self.assertEqual(result.returncode, 0)

    harvest_packages = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type='datajson')

    created = False

    for datasets in harvest_packages:
      for dataset in datasets:
        if dataset['name'] == harvest_source['result']['title']:
          created = True
          logger.info('Found!')
        else:
          logger.info('Other harvest source: {}'.format(dataset['name']))

    self.assertEqual(created, True)

  def test_harvest_local_data(self):
    NAME = 'usda-data'
    base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'
    URL = f'{base_url}/usda.gov.data-3-datasets.json'
    cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
    cpa.delete_all_harvest_sources()

    harvest_source = cpa.create_harvest_source(title=NAME,
                                                url=URL,
                                                owner_org_id=CKAN_ORG_ID,
                                                source_type='datajson',
                                                notes='Some tests about local harvesting sources creation',
                                                frequency='WEEKLY')

    HARVEST_SOURCE_ID = harvest_source['result']['id']

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format(#nosec
      NAME,
      URL,
      HARVEST_SOURCE_ID,
      CKAN_ORG_ID,
      CKAN_BASE_URL,
      CKAN_API_KEY)],
      shell=True,
      stdout=PIPE)

    result.communicate()

    self.assertEqual(result.returncode, 0)

    harvest_packages = cpa.search_harvest_packages(rows=1000, harvest_type='harvest', source_type='datajson')

    created = False

    for datasets in harvest_packages:
      for dataset in datasets:
        if dataset['name'] == harvest_source['result']['title']:
          created = True
          logger.info('Found!')
        else:
          logger.info('Other harvest source: {}'.format(dataset['name']))

    self.assertEqual(created, True)
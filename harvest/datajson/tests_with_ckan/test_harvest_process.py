import sys
import unittest
from harvester import config

from subprocess import call, check_output, Popen, PIPE

from harvester.logs import logger
from harvester.data_gov_api import CKANPortalAPI

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
from bs4 import BeautifulSoup


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

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format(
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
    BASE_URL = 'https://datopian.gitlab.io/ckan-ng-harvest'
    URL_WITH_3_DATASETS = f'{BASE_URL}/usda.gov.data-3-datasets.json'
    REPORT_PATH = 'data/usda-data/final-report.html'
    cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)
    cpa.delete_all_harvest_sources()

    harvest_source = cpa.create_harvest_source(title=NAME,
                                                url=URL_WITH_3_DATASETS,
                                                owner_org_id='california',
                                                source_type='datajson',
                                                notes='Some tests about local harvesting sources creation',
                                                frequency='WEEKLY')

    HARVEST_SOURCE_ID = harvest_source['result']['id']

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format(
      NAME,
      URL_WITH_3_DATASETS,
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

    with open(REPORT_PATH) as rp:
      soup = BeautifulSoup(rp, features="lxml")
    result = soup.find('li',{'class':'results'}).text
    self.assertEqual(result, "create: 3, succeed: 0, fail: 3")

    URL_WITH_2_DATASETS = f'{BASE_URL}/usda.gov.data-2-datasets.json'

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format(
      NAME,
      URL_WITH_2_DATASETS,
      HARVEST_SOURCE_ID,
      CKAN_ORG_ID,
      CKAN_BASE_URL,
      CKAN_API_KEY)],
      shell=True,
      stdout=PIPE)

    result.communicate()

    self.assertEqual(result.returncode, 0)

    with open(REPORT_PATH) as rp:
      soup = BeautifulSoup(rp, features="lxml")
    result = soup.find('li',{'class':'results'}).text
    self.assertEqual(result, "create: 2, succeed: 0, fail: 2")

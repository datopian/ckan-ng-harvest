import os
import sys
import unittest

from subprocess import call, check_output, Popen, PIPE

from logs import logger

from settings import (CKAN_API_KEY,
                       CKAN_BASE_URL,
                       CKAN_ORG_ID,
                       CKAN_VALID_USER_ID
                      )

from libs.data_gov_api import CKANPortalAPI


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

    package_show = cpa.show_package(ckan_package_id_or_name=HARVEST_SOURCE_ID)
   
    self.assertEqual(package_show['success'], True)
    self.assertEqual(package_show['result']['title'], NAME)
    self.assertEqual(package_show['result']['organization']['title'], CKAN_ORG_ID)
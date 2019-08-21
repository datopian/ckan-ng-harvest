import os
import sys
import unittest

from subprocess import call, check_output, Popen, PIPE

from logs import logger

from settings import (HARVEST_SOURCE_ID,
                       CKAN_API_KEY,
                       CKAN_BASE_URL,
                       CKAN_ORG_ID,
                       CKAN_VALID_USER_ID
                      )

class HarvestTestClass(unittest.TestCase):

  def test_harvest(self):
    NAME = 'rrb'
    URL = 'https://secure.rrb.gov/data.json'

    result = Popen(['python3 harvest.py --name {} --url {} --harvest_source_id {} --ckan_owner_org_id {} --catalog_url {} --ckan_api_key {}'.format(
      NAME, 
      URL, 
      HARVEST_SOURCE_ID, 
      CKAN_ORG_ID, 
      CKAN_BASE_URL, 
      CKAN_API_KEY)], 
      shell=True,
      stdout=PIPE)
    
    output,err = result.communicate()

    self.assertEqual(result.returncode, 0)
  
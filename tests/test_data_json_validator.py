import unittest
import json
import config
import requests
from functions import validate_data_json

JSON = {
    "@type": "dcat:Catalog",
    "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
    "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
    "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
    "dataset": [
        {}
    ]
}

DATASET = {
      "identifier": "USDA-DM-002",
      "accessLevel": "public",
      "contactPoint": {
        "hasEmail": "mailto:Alexis.Graves@ocio.usda.gov",
        "@type": "vcard:Contact",
        "fn": "Alexi Graves"
      },
      "programCode": [
        "005:059"
      ],
      "description": "This dataset is Congressional Correspondence from the Office of the Executive Secretariat for the Department of Agriculture.",
      "title": "Department of Agriculture Congressional Logs for Fiscal Year 2014",
      "distribution": [
        {
          "@type": "dcat:Distribution",
          "downloadURL": "http://www.dm.usda.gov/foia/docs/Copy%20of%20ECM%20Congressional%20Logs%20FY14.xls",
          "mediaType": "application/vnd.ms-excel",
          "title": "Congressional Logs for Fiscal Year 2014"
        }
      ],
      "license": "https://creativecommons.org/publicdomain/zero/1.0/",
      "bureauCode": [
        "005:12"
      ],
      "modified": "2014-10-03",
      "publisher": {
        "@type": "org:Organization",
        "name": "Department of Agriculture"
      },
      "keyword": [
        "Congressional Logs"
      ]
    }


class DataJSONValidatorTestClass(unittest.TestCase):

    def test_json_structure(self):
        errors = validate_data_json(JSON)
        self.assertTrue('Bad JSON Structure' in errors[0])
    
    def test_json_empty(self):
        errors = validate_data_json([])
        self.assertTrue('Catalog Is Empty' in errors[0])

    def test_fields_missing(self):
        errors = validate_data_json([JSON])    
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue("The 'accessLevel' field is missing. (1 locations)" in errors[0][1]) 
        self.assertTrue("The 'bureauCode' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'contactPoint' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'description' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'identifier' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'keyword' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'modified' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'programCode' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'publisher' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue("The 'title' field is missing. (1 locations)" in errors[0][1])
    
    def test_email_valid(self):
        JSON["dataset"] = DATASET
        errors = validate_data_json([JSON["dataset"]])
        self.assertTrue("Invalid Required Field Value" in errors[0][0])
        self.assertTrue('The email address "Alexis.Graves@ocio.usda.gov" is not a valid email address. (1 locations)' in errors[0][1])
        
    def test_title_not_empty(self):
        DATASET["title"] = ""
        JSON["dataset"] = DATASET
        errors = validate_data_json([JSON["dataset"]])
        self.assertTrue('Missing Required Fields' in errors[1][0])
        self.assertTrue("The 'title' field is present but empty. (1 locations)" in errors[1][1])
    
    def test_title_not_too_short(self):
        DATASET["title"] = "D"
        JSON["dataset"] = DATASET
        errors = validate_data_json([JSON["dataset"]])
        self.assertTrue('Invalid Field Value' in errors[1][0])
        self.assertTrue('The \'title\' field is very short (min. 2): "D" (1 locations)' in errors[1][1])
import unittest
import json
import config
import requests
from functions import validate_data_json


class DatajsonValidatorTestClass(unittest.TestCase):

    def test_fields_missing(self):
        errors = validate_data_json({})
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'accessLevel' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'bureauCode' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'contactPoint' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'description' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'identifier' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'keyword' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'modified' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'programCode' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'publisher' field is missing. (1 locations)" in errors[0][1])
        self.assertTrue(
            "The 'title' field is missing. (1 locations)" in errors[0][1])

    def test_email_valid(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = "mailto:@example.com"
        errors = validate_data_json(dataset)
        self.assertTrue("Invalid Required Field Value" in errors[0][0])
        self.assertTrue(
            'The email address "@example.com" is not a valid email address. (1 locations)' in errors[0][1])

    def test_title_not_empty(self):
        dataset = self.get_dataset()
        dataset["title"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'title' field is present but empty. (1 locations)" in errors[0][1])

    def test_title_not_too_short(self):
        dataset = self.get_dataset()
        dataset["title"] = "D"
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Field Value' in errors[0][0])
        self.assertTrue(
            'The \'title\' field is very short (min. 2): "D" (1 locations)' in errors[0][1])

    def test_access_level_not_empty(self):
        dataset = self.get_dataset()
        dataset["accessLevel"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'accessLevel' field is present but empty. (1 locations)" in errors[0][1])

    def test_access_level_valid(self):
        dataset = self.get_dataset()
        dataset["accessLevel"] = "super-public"
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The field \'accessLevel\' had an invalid value: "super-public" (1 locations)' in errors[0][1])

    def test_bureu_code_not_empty(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = [""]
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The bureau code "" is invalid. Start with the agency code, then a colon, then the bureau code. (1 locations)' in errors[0][1])

    def test_bureu_code_is_string(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = [2]
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'Each bureauCode must be a string (1 locations)' in errors[0][1])

    def test_bureu_code_known(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = ['005:48']
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The bureau code "005:48" was not found in our list https://project-open-data.cio.gov/data/omb_bureau_codes.csv (1 locations)' in errors[0][1])

    def test_contact_point_not_empty(self):
        dataset = self.get_dataset()
        dataset["contactPoint"] = {}
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'fn' field is missing. (1 locations)" in errors[0][1][0])
        self.assertTrue(
            "The 'hasEmail' field is missing. (1 locations)" in errors[0][1][1])

    def test_contact_point_email_not_empty(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'hasEmail' field is present but empty. (1 locations)" in errors[0][1])

    def test_contact_point_email_valid(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = "mailto:@example.com"
        errors = validate_data_json(dataset)
        self.assertTrue("Invalid Required Field Value" in errors[0][0])
        self.assertTrue(
            'The email address "@example.com" is not a valid email address. (1 locations)' in errors[0][1])

    def test_description_not_empty(self):
        dataset = self.get_dataset()
        dataset["description"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'description' field is present but empty. (1 locations)" in errors[0][1])

    def test_identifier_not_empty(self):
        dataset = self.get_dataset()
        dataset["identifier"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'identifier' field is present but empty. (1 locations)" in errors[0][1])

    def test_keyword_is_array(self):
        dataset = self.get_dataset()
        dataset["keyword"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Update Your File!' in errors[0][0])
        self.assertTrue(
            'The keyword field used to be a string but now it must be an array. (1 locations)' in errors[0][1])

    def test_keyword_not_empty(self):
        dataset = self.get_dataset()
        dataset["keyword"] = []
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'keyword' field is an empty array. (1 locations)" in errors[0][1])

    def test_keywords_are_strings(self):
        dataset = self.get_dataset()
        dataset["keyword"] = ["", 2]
        errors = validate_data_json(dataset)
        self.assertTrue('Invalid Required Field Value' in errors[0][0])
        self.assertTrue('A keyword in the keyword array was an empty string. (1 locations)',
                        'Each keyword in the keyword array must be a string (1 locations)' in errors[0][1])

    def test_modified_not_empty(self):
        dataset = self.get_dataset()
        dataset["keyword"] = []
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'keyword' field is an empty array. (1 locations)" in errors[0][1])

    def get_dataset(self):
        return {
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

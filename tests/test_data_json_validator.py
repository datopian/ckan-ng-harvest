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
            "The 'accessLevel' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'bureauCode' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'contactPoint' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'description' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'identifier' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'keyword' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'modified' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'programCode' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'publisher' field is missing." in errors[0][1])
        self.assertTrue(
            "The 'title' field is missing." in errors[0][1])

    def test_email_valid(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = "mailto:@example.com"
        errors = validate_data_json(dataset)
        self.assertTrue("USDA-DM-002: Invalid Required Field Value" in errors[0][0])
        self.assertTrue(
            'The email address "@example.com" is not a valid email address. (1 locations)' in errors[0][1])

    def test_title_not_empty(self):
        dataset = self.get_dataset()
        dataset["title"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'title' field is present but empty. (1 locations)" in errors[0][1])

    def test_title_not_too_short(self):
        dataset = self.get_dataset()
        dataset["title"] = "D"
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Field Value' in errors[0][0])
        self.assertTrue(
            'The \'title\' field is very short (min. 2): "D" (1 locations)' in errors[0][1])

    def test_access_level_not_empty(self):
        dataset = self.get_dataset()
        dataset["accessLevel"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'accessLevel' field is present but empty. (1 locations)" in errors[0][1])

    def test_access_level_valid(self):
        dataset = self.get_dataset()
        dataset["accessLevel"] = "super-public"
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The field \'accessLevel\' had an invalid value: "super-public" (1 locations)' in errors[0][1])

    def test_bureu_code_not_empty(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = [""]
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The bureau code "" is invalid. Start with the agency code, then a colon, then the bureau code. (1 locations)' in errors[0][1])

    def test_bureu_code_is_string(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = [2]
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'Each bureauCode must be a string (1 locations)' in errors[0][1])

    def test_bureu_code_known(self):
        dataset = self.get_dataset()
        dataset["bureauCode"] = ['005:48']
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The bureau code "005:48" was not found in our list https://project-open-data.cio.gov/data/omb_bureau_codes.csv (1 locations)' in errors[0][1])

    def test_contact_point_not_empty(self):
        dataset = self.get_dataset()
        dataset["contactPoint"] = {}
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'fn' field is missing. (1 locations)" in errors[0][1][0])
        self.assertTrue(
            "The 'hasEmail' field is missing. (1 locations)" in errors[0][1][1])

    def test_contact_point_email_not_empty(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'hasEmail' field is present but empty. (1 locations)" in errors[0][1])

    def test_contact_point_email_valid(self):
        dataset = self.get_dataset()
        dataset["contactPoint"]["hasEmail"] = "mailto:@example.com"
        errors = validate_data_json(dataset)
        self.assertTrue("USDA-DM-002: Invalid Required Field Value" in errors[0][0])
        self.assertTrue(
            'The email address "@example.com" is not a valid email address. (1 locations)' in errors[0][1])

    def test_description_not_empty(self):
        dataset = self.get_dataset()
        dataset["description"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'description' field is present but empty. (1 locations)" in errors[0][1])

    def test_identifier_not_empty(self):
        dataset = self.get_dataset()
        dataset["identifier"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'identifier' field is present but empty." in errors[0][1])

    def test_keyword_is_array(self):
        dataset = self.get_dataset()
        dataset["keyword"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Update Your File!' in errors[0][0])
        self.assertTrue(
            'The keyword field used to be a string but now it must be an array. (1 locations)' in errors[0][1])

    def test_keyword_not_empty(self):
        dataset = self.get_dataset()
        dataset["keyword"] = []
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'keyword' field is an empty array. (1 locations)" in errors[0][1])

    def test_keywords_are_strings(self):
        dataset = self.get_dataset()
        dataset["keyword"] = ["", 2]
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue('A keyword in the keyword array was an empty string. (1 locations)',
                        'Each keyword in the keyword array must be a string (1 locations)' in errors[0][1])

    def test_modified_not_empty(self):
        dataset = self.get_dataset()
        dataset["modified"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'modified' field is present but empty. (1 locations)" in errors[0][1])

    def test_modified_format(self):
        dataset = self.get_dataset()
        dataset["modified"] = "dfsfsdf"
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'The field "modified" is not in valid format: "dfsfsdf" (1 locations)' in errors[0][1])

    def test_programCode_not_empty(self):
        dataset = self.get_dataset()
        dataset["programCode"] = []
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'programCode' field is an empty array. (1 locations)" in errors[0][1])

    def test_programCode_format(self):
        dataset = self.get_dataset()
        dataset["programCode"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            "The 'programCode' field must be a array but it has a different datatype (string). (1 locations)" in errors[0][1])

    def test_programCode_item_is_string(self):
        dataset = self.get_dataset()
        dataset["programCode"] = [2]
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            'Each programCode in the programCode array must be a string (1 locations)' in errors[0][1])

    def test_programCode_item_format(self):
        dataset = self.get_dataset()
        dataset["programCode"] = ["005:9"]
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            'One of programCodes is not in valid format (ex. 018:001): "005:9" (1 locations)' in errors[0][1])

    def test_publisher_is_dictionary(self):
        dataset = self.get_dataset()
        dataset["publisher"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            "The 'publisher' field must be a <class 'dict'> but it has a different datatype (string). (1 locations)" in errors[0][1])

    def test_publisher_has_name(self):
        dataset = self.get_dataset()
        dataset["publisher"] = {}
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'name' field is missing. (1 locations)" in errors[0][1])

    def test_dataQuality_is_bool(self):
        dataset = self.get_dataset()
        dataset["dataQuality"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            'The field \'dataQuality\' must be true or false, as a JSON boolean literal (not the string "true" or "false"). (1 locations)' in errors[0][1])

    def test_distribution_is_array(self):
        dataset = self.get_dataset()
        dataset["distribution"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'distribution' must be an array, if present. (1 locations)" in errors[0][1])

    def test_distribution_required_field_mediaType(self):
        dataset = self.get_dataset()
        dataset["distribution"] = [
            {
                "downloadURL": "",
            }
        ]
        errors = validate_data_json(dataset)
        self.assertTrue('Missing Required Fields' in errors[0][0])
        self.assertTrue(
            "The 'mediaType' field is missing. (1 locations)" in errors[0][1])

    def test_distribution_mediaType(self):
        dataset = self.get_dataset()
        dataset["distribution"] = [
            {
                "downloadURL": "",
                "mediaType": "s",
            }
        ]
        errors = validate_data_json(dataset)
        self.assertTrue('USDA-DM-002 distribution 1: Invalid Field Value' in errors[0][0])
        self.assertTrue(
            'The distribution mediaType "s" is invalid. It must be in IANA MIME format. (1 locations)' in errors[0][1])

    def test_spatial_is_string(self):
        dataset = self.get_dataset()
        dataset["spatial"] = []
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'spatial' must be a string value if specified. (1 locations)" in errors[0][1])

    def test_temporal_is_string(self):
        dataset = self.get_dataset()
        dataset["temporal"] = []
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'temporal' must be a string value if specified. (1 locations)" in errors[0][1])

    def test_accrualPeriodicity_is_valid(self):
        dataset = self.get_dataset()
        dataset["accrualPeriodicity"] = "R/P10"
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'accrualPeriodicity' had an invalid value. (1 locations)" in errors[0][1])

    def test_describedByType_is_valid(self):
        dataset = self.get_dataset()
        dataset["describedByType"] = "l"
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value' in errors[0][0])
        self.assertTrue(
            'The describedByType "l" is invalid. It must be in IANA MIME format. (1 locations)' in errors[0][1])

    def test_isPartOf_is_valid(self):
        dataset = self.get_dataset()
        dataset["isPartOf"] = 2
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Required Field Value' in errors[0][0])
        self.assertTrue(
            "The 'isPartOf' field must be a string but it has a different datatype (<class 'int'>). (1 locations)" in errors[0][1])

    def test_issued_is_valid(self):
        dataset = self.get_dataset()
        dataset["issued"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'issued' is not in a valid format. (1 locations)" in errors[0][1])

    def test_language_is_array(self):
        dataset = self.get_dataset()
        dataset["language"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'language' must be an array, if present. (1 locations)" in errors[0][1])

    def test_language_is_valid(self):
        dataset = self.get_dataset()
        dataset["language"] = ["a"]
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            'The field \'language\' had an invalid language: "a" (1 locations)' in errors[0][1])

    def test_PrimaryITInvestmentUII_is_valid(self):
        dataset = self.get_dataset()
        dataset["PrimaryITInvestmentUII"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'PrimaryITInvestmentUII' must be a string in 023-000000001 format, if present. (1 locations)" in errors[0][1])

    def test_references_is_array(self):
        dataset = self.get_dataset()
        dataset["references"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'references' must be an array, if present. (1 locations)" in errors[0][1])

    def test_theme_is_array(self):
        dataset = self.get_dataset()
        dataset["theme"] = ""
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            "The field 'theme' must be an array. (1 locations)" in errors[0][1])

    def test_theme_items_are_not_empty_strings(self):
        dataset = self.get_dataset()
        dataset["theme"] = [""]
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            'A value in the theme array was an empty string. (1 locations)' in errors[0][1])

    def test_theme_items_are_strings(self):
        dataset = self.get_dataset()
        dataset["theme"] = [2]
        errors = validate_data_json(dataset)
        self.assertTrue(
            'USDA-DM-002: Invalid Field Value (Optional Fields)' in errors[0][0])
        self.assertTrue(
            'Each value in the theme array must be a string (1 locations)' in errors[0][1])

    # TODO test redacted

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

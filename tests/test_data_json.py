import unittest
from libs.data_json import DataJSON
from libs.ckan_dataset_adapters import DataJSONSchema1_1

base_url = 'https://datopian.gitlab.io/ckan-ng-harvest'


class DataJSONTestClass(unittest.TestCase):

    test_original_datajson_datasets = {
            "@type": "dcat:Catalog",
            "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld",
            "dataset": [
                {
                "identifier": "USDA-26522",
                "accessLevel": "public",
                "isPartOf": 'USDA-26521',
                "contactPoint": {
                    "hasEmail": "mailto:dataset2@usda.gov",
                    "@type": "vcard:Contact",
                    "fn": "Dataset Two"
                    },
                "programCode": ["005:044"],
                "description": "Some notes dataset 2 ...",
                "title": "Dataset 2 Title",
                "distribution": [
                    {
                    "@type": "dcat:Distribution",
                    "downloadURL": "http://dataset2.usda.gov/",
                    "mediaType": "text/html",
                    "title": "Web Page Datset 2"
                    },
                    {
                    "@type": "dcat:Distribution",
                    "downloadURL": "http://dataset2.usda.gov/costsavings.json",
                    "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
                    "mediaType": "application/json",
                    "conformsTo": "https://management.cio.gov/schema/",
                    "describedByType": "application/json"
                    }
                ],
                "license": "https://creativecommons.org/licenses/by/4.0",
                "bureauCode": ["005:41"],
                "modified": "2018-12-23",
                "publisher": {
                    "@type": "org:Organization",
                    "name": "Agricultural Marketing Service"
                    },
                "keyword": ["Datset2", "wholesale market"]
                },

                {
                "identifier": "USDA-26521",
                "accessLevel": "public",
                "contactPoint": {
                    "hasEmail": "mailto:Fred.Teensma@ams.usda.gov",
                    "@type": "vcard:Contact",
                    "fn": "Fred Teensma"
                    },
                "programCode": ["005:047"],
                "description": "Some notes ...",
                "title": "Fruit and Vegetable Market News Search",
                "distribution": [
                    {
                    "@type": "dcat:Distribution",
                    "downloadURL": "http://marketnews.usda.gov/",
                    "mediaType": "text/html",
                    "title": "Web Page"
                    },
                    {
                    "@type": "dcat:Distribution",
                    "downloadURL": "http://www.usda.gov/digitalstrategy/costsavings.json",
                    "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
                    "mediaType": "application/json",
                    "conformsTo": "https://management.cio.gov/schema/",
                    "describedByType": "application/json"
                    }
                ],
                "license": "https://creativecommons.org/licenses/by/4.0",
                "bureauCode": ["005:45"],
                "modified": "2014-12-23",
                "publisher": {
                    "@type": "org:Organization",
                    "name": "Agricultural Marketing Service"
                    },
                "keyword": ["FOB", "wholesale market"],

                }
                ]
            }

    def test_load_from_url(self):
        dj = DataJSON()

        ret, error = dj.download_data_json()
        self.assertFalse(ret)  # No URL

        dj.url = f'{base_url}/DO-NOT-EXISTS.json'
        ret, error = dj.download_data_json()
        self.assertFalse(ret)  # URL do not exists (404)

        dj.url = f'{base_url}/bad.json'
        ret, error = dj.download_data_json()
        self.assertTrue(ret)  # URL exists but it's a bad JSON, do not fails, it's downloadable (OK)

    def test_read_json(self):
        dj = DataJSON()

        dj.url = f'{base_url}/bad.json'
        ret, error = dj.download_data_json()

        ret, error = dj.load_data_json()
        self.assertFalse(ret)  # it's a bad JSON

        dj.url = f'{base_url}/good-but-not-data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        self.assertTrue(ret)  # it's a good JSON

    def test_validate_json1(self):

        dj = DataJSON()

        dj.url = f'{base_url}/good-but-not-data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()
        self.assertFalse(ret)  # no schema

    def test_validate_json2(self):
        # data.json without errors
        dj = DataJSON()

        dj.url = f'{base_url}/usda.gov.data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()

        self.assertTrue(ret)  # schema works without errors
        self.assertEqual(None, errors)

    def test_validate_json3(self):
        # data.json with some errors
        dj = DataJSON()

        dj.url = f'{base_url}/healthdata.gov.data.json'
        ret, error = dj.download_data_json()
        ret, error = dj.load_data_json()
        ret, errors = dj.validate_json()

        self.assertFalse(ret)  # schema works but has errors
        self.assertEqual(1, len(errors))  # 1 schema errors

    def test_load_from_data_json_object(self):
        # test loading a data.json dict
        dj = DataJSON()
        dj.read_dict_data_json(data_json_dict=self.test_original_datajson_datasets)
        ret, error = dj.validate_json()
        print(error)

        for dataset in dj.datasets:
            if dataset['identifier'] == 'USDA-26521':
                assert dataset['is_collection'] == True
            if dataset['identifier'] == 'USDA-26522':
                assert dataset['collection_pkg_id'] == ''

    def test_catalog_extras(self):
        assert 'TEST CATALOG EXTRAS' == False


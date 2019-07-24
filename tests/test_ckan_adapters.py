import pytest
from libs.ckan_adapters import DataJSONSchema1_1


class TestCKANAdapter(object):

    def test_datajson_1_1_to_ckan(self):

        dataset = {
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
                }
            ],
            "license": "https://creativecommons.org/licenses/by/4.0",
            "bureauCode": ["005:45"],
            "modified": "2014-12-23",
            "publisher": {
                "@type": "org:Organization",
                "name": "Agricultural Marketing Service, Department of Agriculture"
                },
            "keyword": ["FOB", "wholesale market"],
            "headers": {
            "@type": "dcat:Catalog",
            "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"
            }
        }
        djss = DataJSONSchema1_1(original_dataset=dataset)
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'

        ckan_dataset = djss.transform_to_ckan_dataset()

        assert ckan_dataset['owner_org'] == 'XXXX'
        assert ckan_dataset['notes'] == 'Some notes ...'
        assert len(ckan_dataset['resources']) == 1
        assert ckan_dataset['maintainer_email'] == 'mailto:Fred.Teensma@ams.usda.gov'



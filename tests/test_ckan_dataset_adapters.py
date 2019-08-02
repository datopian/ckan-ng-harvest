import pytest
from libs.ckan_dataset_adapters import DataJSONSchema1_1


class TestCKANDatasetAdapter(object):

    test_datajson_dataset = {
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
            "headers": {
            "@type": "dcat:Catalog",
            "describedBy": "https://project-open-data.cio.gov/v1.1/schema/catalog.json",
            "conformsTo": "https://project-open-data.cio.gov/v1.1/schema",
            "@context": "https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld"
            }
        }

    def test_datajson_1_1_to_ckan(self):

        djss = DataJSONSchema1_1(original_dataset=self.test_datajson_dataset)
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'

        ckan_dataset = djss.transform_to_ckan_dataset()

        assert ckan_dataset['owner_org'] == 'XXXX'
        assert ckan_dataset['notes'] == 'Some notes ...'
        assert len(ckan_dataset['resources']) == 2
        assert ckan_dataset['maintainer_email'] == 'Fred.Teensma@ams.usda.gov'
        assert len(ckan_dataset['tags']) == 2
        assert ckan_dataset['license_id'] == 'cc-by'  # transformation
        # test publisher processor
        assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']
        assert [] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

        # test publisher subOrganizationOf
        t2 = self.test_datajson_dataset
        t2['publisher']['subOrganizationOf'] = {
                        "@type": "org:Organization",
                        "name": "Department of Agriculture"
                        }
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']
        assert ['Department of Agriculture > Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

        t2['publisher']['subOrganizationOf']['subOrganizationOf'] = {
                        "@type": "org:Organization",
                        "name": "USA GOV"
                        }
        djss.original_dataset = t2
        ckan_dataset = djss.transform_to_ckan_dataset()
        assert ['Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher']
        assert ['USA GOV > Department of Agriculture > Agricultural Marketing Service'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'publisher_hierarchy']

    def test_required_fields(self):

        dataset = self.test_datajson_dataset
        # drop required keys
        djss = DataJSONSchema1_1(original_dataset=dataset)
        # ORG is required!

        with pytest.raises(Exception):
            ckan_dataset = djss.transform_to_ckan_dataset()

        djss.ckan_owner_org_id = 'XXXX'
        ckan_dataset = djss.transform_to_ckan_dataset()
        del ckan_dataset['name']

        ret, error = djss.validate_final_dataset(ckan_dataset=ckan_dataset)
        assert ret == False
        assert 'name is a required field' in error

    def test_resources(self):
        djss = DataJSONSchema1_1(original_dataset=self.test_datajson_dataset)
        # ORG is required!
        djss.ckan_owner_org_id = 'XXXX'

        # sample from CKAN results
        existing_resources = [
                { # the first is a real CKAN result from a data.json distribution/resource on test_datajsoin_dataset
                "conformsTo": "https://management.cio.gov/schema/",
                "cache_last_updated": None,
                "describedByType": "application/json",
                "package_id": "d84cac16-307f-4ed9-8353-82d303e2b581",
                "webstore_last_updated": None,
                "id": "d0eb660c-7734-4fe1-b106-70f817f1c99d",
                "size": None,
                "state": "active",
                "describedBy": "https://management.cio.gov/schemaexamples/costSavingsAvoidanceSchema.json",
                "hash": "",
                "description": "costsavings.json",
                "format": "JSON",
                "tracking_summary": {
                "total": 20,
                "recent": 1
                },
                "mimetype_inner": None,
                "url_type": None,
                "revision_id": "55598e72-79d2-4679-8095-aa4b1e67b2f5",
                "mimetype": "application/json",
                "cache_url": None,
                "name": "JSON File",
                "created": "2018-02-03T23:39:07.247009",
                "url": "http://www.usda.gov/digitalstrategy/costsavings.json",
                "webstore_url": None,
                "last_modified": None,
                "position": 0,
                "no_real_name": "True",
                "resource_type": None
                },
                {
                "cache_last_updated": None,
                "package_id": "6fdad934-75a4-44d3-aced-2a69a289356d",
                "webstore_last_updated": None,
                "id": "280dff75-cace-458a-bc4d-ff7c67a8366c",
                "size": None,
                "state": "active",
                "hash": "",
                "description": "Query tool",
                "format": "HTML",
                "tracking_summary": {
                "total": 1542,
                "recent": 41
                },
                "last_modified": None,
                "url_type": None,
                "mimetype": "text/html",
                "cache_url": None,
                "name": "Poverty",
                "created": "2018-02-04T00:02:06.320564",
                "url": "http://www.ers.usda.gov/data-products/county-level-data-sets/poverty.aspx",
                "webstore_url": None,
                "mimetype_inner": None,
                "position": 0,
                "revision_id": "ffb7058b-2606-4a13-9669-ccfde2547ff7",
                "resource_type": None
                }]

        ckan_dataset = djss.transform_to_ckan_dataset(existing_resources=existing_resources)

        assert len(ckan_dataset['resources']) == 2

        # we expect for one dataset with an ID (merged)
        for resource in ckan_dataset['resources']:
            if resource['url'] == 'http://marketnews.usda.gov/':
                assert resource['format'] == 'text/html'
                assert resource['mimetype'] == 'text/html'
                assert resource['description'] == ''
                assert resource['name'] == 'Web Page'
            elif resource['url'] == "http://www.usda.gov/digitalstrategy/costsavings.json":
                assert resource['format'] == 'application/json'
                assert resource['mimetype'] == 'application/json'
                assert resource['description'] == ''
                assert 'name' not in resource
            else:
                assert 'Unexpected URL' == False

    """
    def test_name_collision(self):
        assert "Check for package name collision" == False
    """
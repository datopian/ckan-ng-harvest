import pytest
from harvester.adapters.datasets.csw import CSWDataset


class TestCKANDatasetAdapter(object):

    test_dataset = {
            'title': 'CSW dataset',
            'abstract': 'Some notes about this dataset. Bla, bla, bla',
            'tags': ['Electrity', 'Nuclear energy', 'Investment'],

            'spatial-reference-system': 'EPSG:27700',
            'guid' : 'unique ID 971897198',
            # Usefuls
            'dataset-reference-date': '2019-09-09',
            'metadata-language': 'en',
            'metadata-date': '2019-02-02',
            'coupled-resource': 'coup res',
            'contact-email': 'some@email.com',
            'frequency-of-update': 'WEEKLY',
            'spatial-data-service-type': 'other',

            'use-constraints': ['CC-BY', 'http://licence.com'],
        }

    def test_csw_to_ckan(self):

        dst = CSWDataset(original_dataset=self.test_dataset)
        # ORG is required!
        dst.ckan_owner_org_id = 'XXXX'

        ckan_dataset = dst.transform_to_ckan_dataset()

        assert ckan_dataset['owner_org'] == 'XXXX'
        assert ckan_dataset['notes'] == 'Some notes about this dataset. Bla, bla, bla'
        # TODO assert len(ckan_dataset['resources']) == 2
        # TODO assert ckan_dataset['maintainer_email'] == 'Fred.Teensma@ams.usda.gov'
        assert len(ckan_dataset['tags']) == 3
        # TODO assert ckan_dataset['license_id'] == 'cc-by'  # transformation

        # test *Code
        # TODO assert [['005:45']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'bureauCode']
        # TODO assert [['005:047']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'programCode']

        assert ['EPSG:27700'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-reference-system']
        assert ['unique ID 971897198'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'guid']
        assert ['other'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'spatial-data-service-type']
        assert ['WEEKLY'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'frequency-of-update']
        assert ['some@email.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'contact-email']
        assert ['coup res'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'coupled-resource']
        assert ['2019-02-02'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-date']
        assert ['en'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'metadata-language']
        assert ['2019-09-09'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'dataset-reference-date']

        assert [['CC-BY', 'http://licence.com']] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence']
        assert ['http://licence.com'] == [extra['value'] for extra in ckan_dataset['extras'] if extra['key'] == 'licence_url']

    def test_collections(self):
        pass

    def test_required_fields(self):

        dataset = self.test_dataset
        # drop required keys
        dst = CSWDataset(original_dataset=dataset)
        # ORG is required!

        with pytest.raises(Exception):
            ckan_dataset = dst.transform_to_ckan_dataset()

        dst.ckan_owner_org_id = 'XXXX'
        ckan_dataset = dst.transform_to_ckan_dataset()
        del ckan_dataset['name']

        ret, error = dst.validate_final_dataset()
        assert ret == False
        assert 'name is a required field' in error

    def test_resources(self):
        dst = CSWDataset(original_dataset=self.test_dataset)

        pass
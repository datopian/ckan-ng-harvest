import base64
import json
from abc import ABC, abstractmethod
from harvest import helpers
from tools.results.harvested_source import HarvestedSource


class HarvestDestination(ABC):
    """ main harvester class to inherit """
    def __init__(self, *args, **kwargs):
        # configuration (e.g: CKAN uses validator_schema)
        config = kwargs.get('config', {})  # configuration (e.g validation_schema)
        if type(config) == str:
            self.config = json.loads(config)

    @abstractmethod
    def save_dataset(self, dataset):
        # save final dataset to destination
        pass


class CKANHarvestDestination(HarvestDestination):
    """ main harvester class to inherit """
    def __init__(self, catalog_url, api_key, organization_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.catalog_url = catalog_url
        self.api_key = api_key
        self.organization_id = organization_id

    def save_dataset(self, dataset):
        pass
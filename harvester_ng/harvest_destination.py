import base64
import json
import logging
from abc import ABC, abstractmethod
from harvester_adapters.ckan.api import CKANPortalAPI


logger = logging.getLogger(__name__)


class HarvestDestination(ABC):
    """ main harvester class to inherit """
    def __init__(self, *args, **kwargs):
        # configuration (e.g: CKAN uses validator_schema)
        config = kwargs.get('config', {})  # configuration (e.g validation_schema)
        if type(config) == str:
            self.config = json.loads(config)

    @abstractmethod
    def yield_datasets(self):
        """ get datasets to compare and analyze differences """
        pass

    @abstractmethod
    def save_dataset(self, dataset):
        """ save final dataset to destination """
        pass


class CKANHarvestDestination(HarvestDestination):
    """ main harvester class to inherit """
    def __init__(self, catalog_url, api_key, organization_id, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.catalog_url = catalog_url
        self.api_key = api_key
        self.organization_id = organization_id

    def yield_datasets(self, harvest_source_id, save_results_json_path=None):
        
        logger.info(f'Extracting from harvest source id: {harvest_source_id}')
        cpa = CKANPortalAPI(base_url=self.catalog_url)
        resources = 0

        page = 0
        for datasets in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
            # getting resources in pages of packages
            page += 1
            logger.info('PAGE {} from harvest source id: {}'.format(page, harvest_source_id))
            for dataset in datasets:
                pkg_resources = len(dataset['resources'])
                resources += pkg_resources
                yield(dataset)

        logger.info('{} total resources in harvest source id: {}'.format(resources, harvest_source_id))
        if save_results_json_path is not None:
            cpa.save_packages_list(path=save_results_json_path)

    def save_dataset(self, dataset):
        pass
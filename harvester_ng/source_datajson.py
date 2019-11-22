import os
import hashlib
import json
import logging
import pytz
import requests

from datetime import datetime
from dataflows import Flow, add_field, load, update_resource

from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.datajson.harvester import DataJSON
from harvesters.datajson.ckan.dataset import DataJSONSchema1_1
from harvesters import config

from harvester_ng.harvest_source import HarvestSource
from harvester_ng.datajson.flows import (clean_duplicated_identifiers,
                                    validate_datasets,
                                    save_as_data_packages,
                                    compare_resources,
                                    write_results_to_ckan,
                                    assing_collection_pkg_id)

logger = logging.getLogger(__name__)
DEFAULT_VALIDATION_SCHEMA = 'federal-v1.1'


class HarvestDataJSON(HarvestSource):
    """ DataJSON harvester """

    def __init__(self, name, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.validator_schema = self.config.get('validator_schema', DEFAULT_VALIDATION_SCHEMA)

    def download(self):
        # donwload, validate and save as data packages
        res = Flow(
            # get data.json and yield all datasets
            # validate headers and save the validation errors
            self.get_data_json_from_url(validator_schema=self.validator_schema),
            update_resource('res_1', name='datajson', path='datajson.csv'),

            # remove duplicates
            clean_duplicated_identifiers,

            # validate each dataset
            validate_datasets,

            # save each dataset as data package
            save_as_data_packages,
        ).results()

        return res

    def compare(self):
        # compare new vs previous resources
        res = Flow(
            # add other resource to this process. The packages list from data.gov
            self.get_current_ckan_resources_from_api(harvest_source_id=config.SOURCE_ID),
            update_resource('res_1', name='ckan_results'),
            # new field at this copy for comparasion results
            add_field(name='comparison_results',
                      type='object',
                      resources='ckan_results'),

            # Compare both resources
            # In data.json the datasets have the identifier field: "identifier": "USDA-ERS-00071"
            # In CKAN API results the datasets have the same identifier at "extras" list: {"key": "identifier", "value": "USDA-ERS-00071"}
            compare_resources,
        ).results()

    def write_destination(self):
        res = Flow(
            load(load_source=config.get_flow2_datasets_result_path()),
            write_results_to_ckan,
            assing_collection_pkg_id,
        ).results()

    def write_final_report(self):
        # write final process result as JSON
        pass

    def get_data_json_from_url(self, validator_schema):
        logger.info(f'Geting data.json from {self.url}')

        datajson = DataJSON()
        datajson.url = self.url

        try:
            datajson.fetch(timeout=90)
            ret = True
        except Exception as e:
            ret = False
        if not ret:
            error = 'Error getting data: {}'.format(datajson.errors)
            datajson.save_errors(path=config.get_errors_path())
            logger.error(error)
            raise Exception(error)
        logger.info('Downloaded OK')

        ret = datajson.validate(validator_schema=validator_schema)
        if not ret:
            error = 'Error validating data: {}'.format(datajson.errors)
            logger.error(error)
            raise Exception(error)
        else:
            datajson.post_fetch()
            logger.info('Validate OK: {} datasets'.format(len(datajson.datasets)))

        logger.info('{} datasets found'.format(len(datajson.datasets)))

        # save data.json
        datajson.save_json(path=config.get_data_cache_path())
        # save headers errors
        datajson.save_errors(path=config.get_errors_path())

        # the real dataset list

        if config.LIMIT_DATASETS > 0:
            datajson.datasets = datajson.datasets[:config.LIMIT_DATASETS]
        for dataset in datajson.datasets:
            # add headers (previously called catalog_values)
            dataset['headers'] = datajson.headers
            dataset['validator_schema'] = validator_schema
            yield(dataset)

    def get_current_ckan_resources_from_api(harvest_source_id):
        results_json_path = config.get_ckan_results_cache_path()
        logger.info(f'Extracting from harvest source id: {harvest_source_id}')
        cpa = CKANPortalAPI(base_url=config.CKAN_CATALOG_URL)
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
        cpa.save_packages_list(path=results_json_path)

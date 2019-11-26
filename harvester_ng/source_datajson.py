import os
import hashlib
import json
import logging
import pytz
import requests

from datetime import datetime
from dataflows import Flow, add_field, load, update_resource, printer

from harvesters.datajson.harvester import DataJSON
from harvesters import config

from harvester_ng.logs import logger
from harvester_ng.harvest_source import HarvestSource
from harvester_ng.datajson.flows import (clean_duplicated_identifiers,
                                    validate_datasets,
                                    save_as_data_packages,
                                    compare_resources)
from harvester_ng.datajson.flows_ckan import (write_results,
                                              assing_collection_pkg_id)


logger = logging.getLogger(__name__)
DEFAULT_VALIDATOR_SCHEMA = 'federal-v1.1'


class HarvestDataJSON(HarvestSource):
    """ DataJSON harvester """

    def __init__(self, name, *args, **kwargs):
        super().__init__(name, *args, **kwargs)
        self.validator_schema = self.config.get('validator_schema', DEFAULT_VALIDATOR_SCHEMA)

    def download(self):
        # donwload, validate and save as data packages
        logger.info(f'Downloading from data.json source {self.url}')
        save_to = self.get_data_packages_folder_path()
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
            save_as_data_packages(path=save_to),
        ).results()

        return res

    def compare(self):
        # compare new vs previous resources
        logger.info(f'Comparing resources')
        data_packages_path = self.get_data_packages_folder_path()
        res = Flow(
            # add other resource to this process. The packages list from data.gov
            self.get_current_ckan_resources_from_api(harvest_source_id=self.destination.harvest_source_id),
            update_resource('res_1', name='ckan_results'),
            # new field at this copy for comparasion results
            add_field(name='comparison_results',
                      type='object',
                      resources='ckan_results'),

            # Compare both resources
            # In data.json the datasets have the identifier field: "identifier": "USDA-ERS-00071"
            # In CKAN API results the datasets have the same identifier at "extras" list: {"key": "identifier", "value": "USDA-ERS-00071"}
            compare_resources(data_packages_path=data_packages_path),
        ).results()

        return res

    def write_destination(self):
        source = self.get_comparison_result_path()
        logger.info(f'Writting to destination: {self.destination} data from {source}')
        
        res = Flow(
            load(load_source=source),
            # fails self.destination.write_results,
            write_results(self.destination),

            # fails self.destination.assing_collection_pkg_id,
            assing_collection_pkg_id(self.destination),

        ).results()
    
        return res

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
            datajson.save_errors(path=self.get_errors_path())
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
        datajson.save_json(path=self.get_data_cache_path())
        # save headers errors
        datajson.save_errors(path=self.get_errors_path())

        # the real dataset list

        if self.limit_datasets > 0:
            datajson.datasets = datajson.datasets[:self.limit_datasets]
        for dataset in datajson.datasets:
            # add headers (previously called catalog_values)
            dataset['headers'] = datajson.headers
            dataset['validator_schema'] = validator_schema
            yield(dataset)

    def get_current_ckan_resources_from_api(self, harvest_source_id):
        save_results_json_path = self.get_ckan_results_cache_path()
        logger.info(f'Getting destination resources {self.url}')
        dest = self.destination.yield_datasets(harvest_source_id=harvest_source_id, save_results_json_path=save_results_json_path)
        for dataset in dest:
            logger.info(f'Destination resource: {dataset}')
            yield dataset

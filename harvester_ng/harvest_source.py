import base64
import hashlib
import json
import os
from abc import ABC, abstractmethod
from harvester_ng import helpers
from tools.results.harvested_source import HarvestedSource
from slugify import slugify


class HarvestSource(ABC):
    """ main harvester class to inherit """
    def __init__(self, name, *args, **kwargs):
        self.name = name  # name of the harvest source
        self.url = kwargs.get('url', None)  # url to harvest from
        config = kwargs.get('config', {})  # configuration (e.g validation_schema)
        if type(config) == str:
            self.config = json.loads(config)
        else:
            self.config = config

    @abstractmethod
    def download(self):
        # donwload, validate and save as data packages
        # returns a DataFlows resource
        pass

    def save_download_results(self, flow_results):
        # save results (data package and final datasets results)
        dmp = json.dumps(flow_results[0][0], indent=2)
        f = open(self.get_download_result_path(), 'w')
        f.write(dmp)
        f.close()

        pkg = flow_results[1]  # package returned
        pkg.save(self.get_data_package_result_path())

    @abstractmethod
    def compare(self):
        # compare downloaded with destination
        pass

    def save_compare_results(self, flow_results):
        dmp = json.dumps(flow_results[0][0], indent=2)
        f = open(helpers.get_flow2_datasets_result_path(), 'w')
        f.write(dmp)
        f.close()

        pkg = flow_results[1]  # package returned
        pkg.save(helpers.get_flow2_data_package_result_path())

    @abstractmethod
    def write_destination(self):
        # save changes to destination
        pass

    def save_write_results(self, flow_results):
        # save results
        dmp = json.dumps(flow_results[0][0], indent=2)
        f = open(helpers.get_flow2_datasets_result_path(), 'w')
        f.write(dmp)
        f.close()

    @abstractmethod
    def write_final_report(self):
        # write final process result as JSON
        hs = HarvestedSource(self.name)
        hs.process_results()

        # write results
        results = hs.get_json_data()
        f = open(config.get_final_json_results_for_report_path(), 'w')
        f.write(json.dumps(results, indent=2))
        f.close()

        hs.render_template(save=True)

    def hash_dataset(self, dataset):
        # hash the dataset.
        dmp_dataset = json.dumps(dataset, sort_keys=True)
        str_to_hash = dmp_dataset.encode('utf-8')
        return hashlib.sha256(str_to_hash).hexdigest()

    def get_base_path(self):
        # get path for some resource (described as string)
        # if none, retur the base folder
        nice_name = slugify(self.name)
        base_path = os.path.join('data', nice_name)

        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        return base_path

    # get_html_report_path = get_file(resource='final-report.html')
    # get_final_json_results_for_report_path =  'final-results.json'
    def get_file(self, resource, create=True):
        path = os.path.join(self.get_base_path(), resource)
        if create and not os.path.isfile(path):
            open(path, 'w').close()
        return path
    
    def get_data_packages_folder_path(self):
        """ local path for datapackages """
        data_packages_folder_path = os.path.join(self.get_base_path(), 'data-packages')
        if not os.path.isdir(data_packages_folder_path):
            os.makedirs(data_packages_folder_path)

        return data_packages_folder_path
    
    def get_download_result_path(self, create=True):
        """ local path for flow1 results file """
        return self.get_file(resource='download-results.json', create=create)

    def get_data_package_result_path(self, create=True):
        """ local path for flow1 file """
        return self.get_file(resource='data-package-result.json', create=create)

    '''
    def get_data_cache_path(create=True):
        """ local path for data.json source file """
        path = os.path.join(get_base_path(), 'data.json')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path

    def get_flow2_data_package_result_path(create=True):
        """ local path for flow2 data packages results file """
        path = os.path.join(get_base_path(), 'flow2-data-package-result.json')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path

    def get_flow2_datasets_result_path(create=True):
        path = os.path.join(get_base_path(), 'flow2-datasets-results.json')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path


    def get_errors_path(create=True):
        """ local path for errors """
        path =  os.path.join(get_base_path(), 'errors.json')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path


    def get_ckan_results_cache_path(create=True):
        """ local path for ckan results file """
        path = os.path.join(get_base_path(), 'ckan-results.json')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path


    def get_comparison_results_path(create=True):
        """ local path for comparison results file """
        path = os.path.join(get_base_path(), 'compare-results.csv')
        if not os.path.isfile(path):
            open(path, 'w').close()
        return path


    def get_flow2_data_package_folder_path():
        """ local path for flow2 file """
        flow2_data_package_folder_path = os.path.join(get_base_path(), 'flow2')
        if not os.path.isdir(flow2_data_package_folder_path):
            os.makedirs(flow2_data_package_folder_path)

        return flow2_data_package_folder_path


    def get_harvest_sources_path(hs_name):
        base_path = os.path.join(DATA_FOLDER_PATH, 'harvest_sources/datasets')

        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        final_path = os.path.join(base_path, f'harvest-source-{hs_name}.json')

        return final_path


    def get_harvest_sources_data_folder(source_type, name):
        base_path = os.path.join(DATA_FOLDER_PATH, 'harvest_sources', source_type)

        if not os.path.isdir(base_path):
            os.makedirs(base_path)

        return base_path


    def get_harvest_sources_data_path(source_type, name, file_name):
        base_path = get_harvest_sources_data_folder(source_type, name)
        final_path = os.path.join(base_path, file_name)

        return final_path


    def get_json_data_or_none(path):
        if not os.path.isfile(path):
            return None
        else:
            f = open(path, 'r')
            try:
                j = json.load(f)
            except Exception as e:
                j = {'error': str(e)}
            f.close()
            return j

    def get_report_files():
        # collect important files to write a final report
        data_file = get_data_cache_path(create=False)
        results_file = get_flow2_datasets_result_path(create=False)
        errors_file = get_errors_path(create=False)

        return {'data': get_json_data_or_none(data_file),
                'results': get_json_data_or_none(results_file),
                'errors': get_json_data_or_none(errors_file)
                }

    '''




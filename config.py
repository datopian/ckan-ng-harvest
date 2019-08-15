import os
from slugify import slugify

DATA_FOLDER_PATH = 'data'
SOURCE_NAME = ''  # the source nage, e.g. Dep of Agriculture
SOURCE_ID = ''  # the harvest source id
SOURCE_URL = ''  # url of the data.json file
LIMIT_DATASETS = 0  # Limit datasets to harvest on each source. Defualt=0 => no limit"

CKAN_CATALOG_URL = ''  # 'https://catalog.data.gov'
CKAN_API_KEY = ''
CKAN_OWNER_ORG = ''  # ID of the orginazion sharing their data to a CKAN instance


def get_base_path():
    nice_name = slugify(SOURCE_NAME)
    base_path = os.path.join(DATA_FOLDER_PATH, nice_name)

    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    return base_path


def get_datajson_cache_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'data.json')


def get_flow1_data_package_result_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'flow1-data-package-result.json')


def get_flow2_data_package_result_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'flow2-data-package-result.json')

def get_flow1_datasets_result_path(create=True):
    """ local path for data.json source file """
    path = os.path.join(get_base_path(), 'flow1-datasets-results.json')
    if not os.path.isfile(path):
        open(path, 'w').close()
    return path

def get_flow2_datasets_result_path():
    return os.path.join(get_base_path(), 'flow2-datasets-results.json')


def get_datajson_headers_validation_errors_path():
    """ local path for data-json-errors.json source file """
    return os.path.join(get_base_path(), 'data-json-headers-errors.json')

def get_datajson_validation_errors_path():
    """ local path for data-json-errors.json source file """
    return os.path.join(get_base_path(), 'data-json-errors.json')

def get_ckan_results_cache_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'ckan-results.json')


def get_comparison_results_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'compare-results.csv')


def get_data_packages_folder_path():
    """ local path for data.json source file """
    data_packages_folder_path = os.path.join(get_base_path(), 'data-packages')
    if not os.path.isdir(data_packages_folder_path):
        os.makedirs(data_packages_folder_path)

    return data_packages_folder_path


def get_flow2_data_package_folder_path():
    """ local path for data.json source file """
    flow2_data_package_folder_path = os.path.join(get_base_path(), 'flow2')
    if not os.path.isdir(flow2_data_package_folder_path):
        os.makedirs(flow2_data_package_folder_path)

    return flow2_data_package_folder_path

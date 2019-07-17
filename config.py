import os
from slugify import slugify

DATA_FOLDER_PATH = 'data'
SOURCE_NAME = ''  # the source nage, e.g. Dep of Agriculture
SOURCE_ID = ''  # the harvest source id
SOURCE_URL = ''  # url of the data.json file


def get_base_path():
    nice_name = slugify(SOURCE_NAME)
    base_path = os.path.join(DATA_FOLDER_PATH, nice_name)

    if not os.path.isdir(base_path):
        os.makedirs(base_path)

    return base_path


def get_datajson_cache_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'data.json')


def get_datajson_headers_validation_errors_path():
    """ local path for data-json-errors.json source file """
    return os.path.join(get_base_path(), 'data-json-headers-errors.json')


def get_ckan_results_cache_path():
    """ local path for data.json source file """
    return os.path.join(get_base_path(), 'ckan-results.json')


def get_data_packages_folder_path():
    """ local path for data.json source file """
    data_packages_folder_path = os.path.join(get_base_path(), 'data-packages')
    if not os.path.isdir(data_packages_folder_path):
        os.makedirs(data_packages_folder_path)

    return data_packages_folder_path
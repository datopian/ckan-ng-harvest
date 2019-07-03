"""
Get updated list of resources from a data.json file
Get the actual list of resources in data.gov
Compare both and upgrade data.gov 

Using:
 - DataFlows: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md
 - DataPackages: https://frictionlessdata.io/data-packages/ + https://github.com/frictionlessdata/datapackage-py
"""

from dataflows import Flow, printer, dump_to_path, checkpoint
from datapackage import Package
from functions import (get_data_json_from_url, clean_duplicated_identifiers,
                        list_parents_and_childs, get_actual_ckan_resources_from_api)
import json
import os
from logs import logger

resources_path = 'list-resources.json'  # High priority data.json file. 
#TODO get full list from https://catalog.data.gov/api/3/action/package_search?q=%28type:harvest%29&rows=1000
resources_file = open(resources_path, 'r')
json_resources = json.load(resources_file)

# process al harvest sources
for data_json_to_process in json_resources:
    
    name = data_json_to_process['name']  # Nice name of the source
    url = data_json_to_process['url']  # data.json final URL
    source_identifier = data_json_to_process['identifier']  # harves source id at CKAN

    # ----------------------------------------------------
    # Get data.json
    # ----------------------------------------------------
    datajson_package_folder_path = f'datapackages/{name}/datajson'
    datajson_package_file_path = f'{datajson_package_folder_path}/datapackage.json'
    # just download one time
    if os.path.isfile(datajson_package_file_path):
        logger.info(f'\n-----------------------\n{name}\nData JSON already exist: ignored download')
        datajson_package = Package(datajson_package_file_path)
    else:

        # get resources from data.json file
        Flow(
            get_data_json_from_url(url),
            clean_duplicated_identifiers,

            dump_to_path(datajson_package_folder_path),
            # printer(num_rows=1), # , tablefmt='html')
            
        ).process()[1]

        datajson_package = Package(datajson_package_file_path)

    # ----------------------------------------------------
    # Get resources from API
    # ----------------------------------------------------

    dataapi_package_folder_path = f'datapackages/{name}/dataapi'
    dataapi_package_file_path = f'{dataapi_package_folder_path}/datapackage.json'
    
    # just download one time
    if os.path.isfile(dataapi_package_file_path):
        logger.info(f'\n-----------------------\n{name}\nData API already exist: ignored download')
        dataapi_package = Package(dataapi_package_file_path)
    else:
        # get actual resources at data.gov from API
        Flow(
            get_actual_ckan_resources_from_api(harvest_source_id=source_identifier),
            
            dump_to_path(dataapi_package_folder_path),
            # printer(num_rows=1), # , tablefmt='html')
            
        ).process()[1]

        dataapi_package = Package(dataapi_package_file_path)

        

    """
    # merge both
    Flow(
        
        # define parents and childs identifiers https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145
        # define catalog_extras https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L153

        # Iterate ALL Harvest Objects at CKAN instance https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L164
            # and list which still exist https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L174
            # create a separate list for which are parents
        
        # detect which was demoted and which was promoted to parent
        # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L179
        # error if one opackage is parent and child (identifier is part of both lists) https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L198

        # detect new parents (this one who are parents in data.json but not before) https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L202

        # delete all no longer in the remote catalog https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L322

    ).process()[1]
    """

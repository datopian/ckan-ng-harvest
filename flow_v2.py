"""
Get updated list of resources from a data.json file
Get the actual list of resources in data.gov
Compare both and upgrade data.gov 

Using:
 - DataFlows: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md
 - DataPackages: https://frictionlessdata.io/data-packages/ + https://github.com/frictionlessdata/datapackage-py
"""

from dataflows import Flow, printer, dump_to_path, load, update_resource
# from dataflows.join import join_with_self
import json
import os
from logs import logger
from functions_v2 import (get_data_json_from_url, 
                            clean_duplicated_identifiers,
                            get_actual_ckan_resources_from_api,
                            dbg_packages
                            )

name = 'Energy'
url = 'http://www.energy.gov/data.json'
source_identifier = '8d4de31c-979c-4b50-be6b-ea3c72453ff6'

# name = 'Agriculture'
# url = 'http://www.usda.gov/data.json'
# source_identifier = '50ca39af-9ddb-466d-8cf3-84d67a204346'

data_folder_path = f'data/{name}'
datajson_package_folder_path = f'{data_folder_path}/datapackages'

if not os.path.isdir(datajson_package_folder_path):
    os.makedirs(datajson_package_folder_path)

Flow(
    # not working (why?) 
    # load(load_source=url, name='datajson', datasets='datastes'),
    get_data_json_from_url(url, name=name, path=data_folder_path),  # I like to split headers and ['dataset'] in two resources. Valid the headers from one side and process the ['dataset'] as rows independently. It's a good idea?
    update_resource('res_1', name='datajson'),
    
    clean_duplicated_identifiers,  # when a processor has a _rows_ param, How do you know from which resource specifically the rows should be processed?
    # replace with "join_with_self" join_with_self(source_name, source_key, target_name, fields),
    
    dbg_packages,  # get info about packaghes

    # add other resource to this process. The packages list from data.gov
    get_actual_ckan_resources_from_api(harvest_source_id=source_identifier),
    update_resource('res_2', name='ckanapi'),
    
    dbg_packages,  # get info about updated packaghes
    
    # dump_to_path(datajson_package_folder_path),
    # printer(num_rows=1), # , tablefmt='html')
    
).process()[1]
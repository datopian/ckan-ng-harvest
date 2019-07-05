"""
Get updated list of resources from a data.json file
Get the actual list of resources in data.gov
Compare both and upgrade data.gov 

Using:
 - DataFlows: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md
 - DataPackages: https://frictionlessdata.io/data-packages/ + https://github.com/frictionlessdata/datapackage-py
"""

from dataflows import Flow, printer, dump_to_path, load
import json
import os
from logs import logger
from functions_v2 import (get_data_json_from_url, 
                            clean_duplicated_identifiers,
                            validate_headers,
                            split_headers,
                            get_actual_ckan_resources_from_api,
                            dbg_packages
                            )

name = 'Energy'
url = 'http://www.energy.gov/data.json'
source_identifier = '8d4de31c-979c-4b50-be6b-ea3c72453ff6'

name = 'Agriculture'
url = 'http://www.usda.gov/data.json'
source_identifier = '50ca39af-9ddb-466d-8cf3-84d67a204346'


# ----------------------------------------------------
# Get data.json
# ----------------------------------------------------

datajson_package_folder_path = f'datapackages/{name}/datajson'

Flow(
    # not working (why?) load(load_source=url, name='datajson'),
    get_data_json_from_url(url),  # I like to split headers and ['dataset'] in two resources. Valid the headers from one side and process the ['dataset'] as rows independently. It's a good idea?
    # split_headers,  # split the data.json in headers and the ['dataset'] list in two resources
    # validate_headers,  # validate just the data.json headers (splited in the previous step)
    clean_duplicated_identifiers,  # when a processor has a _rows_ param, How do you know from which resource specifically the rows should be processed?
    
    dbg_packages,  # get info about packaghes

    # add other resource to this process. The packages list from data.gov
    get_actual_ckan_resources_from_api(harvest_source_id=source_identifier),

    dbg_packages,  # get info about packaghes
    
    # dump_to_path(datajson_package_folder_path),
    # printer(num_rows=1), # , tablefmt='html')
    
).process()[1]
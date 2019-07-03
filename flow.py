"""
Get updated list of resources from a data.json file
Get the actual list of resources in data.gov
Compare both and upgrade data.gov 

Using:
 - DataFlows: https://github.com/datahq/dataflows/blob/master/TUTORIAL.md
 - DataPackages: https://frictionlessdata.io/data-packages/
"""

from dataflows import Flow, printer, dump_to_path, checkpoint
from functions import (get_data_json_from_url, clean_duplicated_identifiers,
                        list_parents_and_childs, get_actual_ckan_resources_from_api)
import json

resources_path = 'list-resources.json'  # High priority data.json file. 
#TODO get full list from https://catalog.data.gov/api/3/action/package_search?q=%28type:harvest%29&rows=1000
resources_file = open(resources_path, 'r')
json_resources = json.load(resources_file)

# process al harvest sources
for data_json_to_process in json_resources:
    
    name = data_json_to_process['name']  # Nice name of the source
    url = data_json_to_process['url']  # data.json final URL
    source_identifier = data_json_to_process['identifier']  # harves source id at CKAN

    # get resources from data.json file
    Flow(
        get_data_json_from_url(url),
        clean_duplicated_identifiers,

        dump_to_path(f'datapackages/{name}/datajson'),
        # printer(num_rows=1), # , tablefmt='html')
        
    ).process()[1]

    # get actual resources at data.gov from API
    Flow(
        get_actual_ckan_resources_from_api(harvest_source_id=source_identifier),
        
        dump_to_path(f'datapackages/{name}/dataapi'),
        # printer(num_rows=1), # , tablefmt='html')
        
    ).process()[1]

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

from dataflows import Flow, printer, dump_to_path, checkpoint
from functions import get_data_json_from_url, clean_duplicated_identifiers, list_parents_and_childs
import json

resources_path = 'list-resources.json'  # High priority data.json file
resources_file = open(resources_path, 'r')
json_resources = json.load(resources_file)

# try the first one
name = json_resources[0]['name']
url = json_resources[0]['url']

Flow(
    get_data_json_from_url(url),
    clean_duplicated_identifiers,
    dump_to_path(f'datapackages/package_{name}'),
    checkpoint('data-json-cleaned-duplicates'),
    # printer(num_rows=1), # , tablefmt='html')
    
).process()[1]

"""
# learn how to https://github.com/datahq/dataflows/blob/master/TUTORIAL.md

# continue working
Flow(
    checkpoint('data-json-cleaned-duplicates'),
    list_parents_and_childs,
).process()[1]
"""
from dataflows import Flow, printer, dump_to_path
from functions import get_data_json_from_url, clean_duplicated_identifiers, list_parents
import json

resources_path = 'list-resources.json'  # High priority data.json file
resources_file = open(resources_path, 'r')
json_resources = json.load(resources_file)

name = json_resources[0]['name']
url = json_resources[0]['url']

Flow(
    get_data_json_from_url(url),
    clean_duplicated_identifiers,
    # list_parents,
    # printer(num_rows=1), # , tablefmt='html')
    dump_to_path(f'datapackages/package_{name}'),
).process()[1]
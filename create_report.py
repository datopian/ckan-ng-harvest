"""
TEST CREATE REPORT.
Read ALL results files about the harvest process and write a report
"""
from jinja2 import Template
import os
import json


context = {}

name = 'agriculture'
base_folder = f'data/{name}'

data_json_file_path = f'{base_folder}/data.json'
if not os.path.isfile(data_json_file_path):
    # ERROR !
    data_json = {'error': 'data.json do not exists'}
else:
    data_json_file = open(data_json_file_path, 'r')
    data_json = json.load(packages_file)

context['data_json'] = data_json

f = open('templates/harvest-report.html')
template = Template(f.read())
template.render(**context)

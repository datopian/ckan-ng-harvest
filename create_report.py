"""
Read ALL results files about the harvest process and write a report
"""
from jinja2 import Template
import os
import json
import config
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)")
args = parser.parse_args()

context = {}
config.SOURCE_NAME = args.name
context['name'] = config.SOURCE_NAME
reports = config.get_report_files()
context.update(reports)

# analize results
results = context['results']
actions = {}  # create | delete | update
validation_errors = []
for result in results:
    comparison_results = result['comparison_results']
    action = comparison_results['action']
    if action not in actions.keys():
        actions[action] = {'total': 0, 'success': 0, 'fails': 0}
    actions[action]['total'] += 1

    if len(comparison_results['new_data'].get('validation_errors', [])) > 0:
        validation_errors += comparison_results['new_data']['validation_errors']

    action_results = comparison_results['action_results']
    success = action_results['success']
    if success:
        actions[action]['success'] += 1
    else:
        actions[action]['fails'] += 1

    warnings = action_results['warnings']
    errors = action_results['errors']

context['actions'] = actions
context['validation_errors'] = validation_errors

f = open('templates/harvest-report.html')
template = Template(f.read())

f = open(config.get_html_report_path(), 'w')
f.write(template.render(**context))
f.close()

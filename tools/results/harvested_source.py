import logging
import os
from jinja2 import Template

from harvester_ng.logs import logger


logger = logging.getLogger(__name__)


class HarvestedSource:
    """
    analyze all data from a particular previously harvested source
    """
    name = None  # source name (and folder name)
    data = None  # data dict
    results = None  # harvest results
    errors = None
    final_results = {}  # all files processed

    def __init__(self, harvest_source_obj):
        self.harvest_source = harvest_source_obj
        self.name = self.harvest_source.name
        data = self.harvest_source.get_report_files()
        self.data = data['data']
        self.results = data['results']
        self.errors = data['errors']

    def process_results(self):

        # analyze results

        actions = {}  # create | delete | update
        validation_errors = []
        action_errors = []
        action_warnings = []
        # print(f'Result: {self.results}')
        if type(self.results) != list:
            logger.error(f'Unexpected results: {self.results}')
            return False

        for result in self.results:

            # print(f'Result: {result}')
            comparison_results = result.get('comparison_results', None)
            if comparison_results is None:
                # this is bad. This source is broken
                return False
            action = comparison_results['action']
            if action not in actions.keys():
                actions[action] = {'total': 0, 'success': 0, 'fails': 0}
            actions[action]['total'] += 1

            if action in ['create', 'update']:  # delete has no new_data
                if len(comparison_results['new_data'].get('validation_errors', [])) > 0:
                    validation_errors += comparison_results['new_data']['validation_errors']

            action_results = comparison_results.get('action_results', {})
            success = action_results.get('success', False)
            if success:
                actions[action]['success'] += 1
            else:
                actions[action]['fails'] += 1

            action_warnings += action_results.get('warnings', [])
            action_errors += action_results.get('errors', [])

        self.final_results['actions'] = actions
        self.final_results['validation_errors'] = validation_errors
        self.final_results['action_warnings'] = action_warnings
        self.final_results['action_errors'] = action_errors

        return True

    def get_json_data(self):
        data = {
            'name': self.name,
            'data': self.data,
            'results': self.results,
            'errors': self.errors,
            'actions': self.final_results.get('actions', {}),
            'validation_errors': self.final_results.get('validation_errors', {}),
            'action_warnings': self.final_results.get('action_warnings', {}),
            'action_errors': self.final_results.get('action_errors', {})
        }
        return data

    def render_template(self, save=True):
        # redenr through harvest-report.html
        context = self.get_json_data()
        logger.info('Errors {}'.format(context['errors']))
        logger.info('Validation errors {}'.format(context['validation_errors']))
        path = 'templates/harvest-report.html'
        template_path = os.path.join(os.path.dirname(__file__), path)
        # template_path = pkg_resources.resource_filename(__name__, path)
        f = open(template_path, 'r')
        template = Template(f.read())
        f.close()
        html = template.render(**context)
        if save:
            report_path = self.harvest_source.get_html_report_path()
            self.save_report(html=html, report_path=report_path)
            logger.info(f'Saved report to {report_path}')

        return html

    def save_report(self, html, report_path):
        f = open(report_path, 'w')
        f.write(html)
        f.close()

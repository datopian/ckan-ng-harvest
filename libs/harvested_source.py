import config
from jinja2 import Template


class HarvestedSource:
    """
    Analize all data from a previously harvested source
    """
    name = None  # source name (and folder name)

    data_json = None  # data.json dict
    results = None  # harvest results
    data_json_validation_errors = None

    final_results = {}  # all files processed

    def __init__(self, name):
        config.SOURCE_NAME = name
        self.name = name
        data = config.get_report_files()
        self.data_json = data['data_json']
        self.results = data['results']
        self.data_json_validation_errors = data['data_json_validation_errors']

    def process_results(self):

        # analyze results

        actions = {}  # create | delete | update
        validation_errors = []
        action_errors = []
        action_warnings = []
        for result in self.results:
            # print(f'Result: {self.results}')
            comparison_results = result['comparison_results']
            action = comparison_results['action']
            if action not in actions.keys():
                actions[action] = {'total': 0, 'success': 0, 'fails': 0}
            actions[action]['total'] += 1

            if action in ['create', 'update']:  # delete has no new_data
                if len(comparison_results['new_data'].get('validation_errors', [])) > 0:
                    validation_errors += comparison_results['new_data']['validation_errors']

            action_results = comparison_results['action_results']
            success = action_results['success']
            if success:
                actions[action]['success'] += 1
            else:
                actions[action]['fails'] += 1

            action_warnings += action_results.get('warnings', [])
            action_errors += action_results['errors']

        self.final_results['actions'] = actions
        self.final_results['validation_errors'] = validation_errors
        self.final_results['action_warnings'] = action_warnings
        self.final_results['action_errors'] = action_errors

    def get_json_data(self):
        data = {
            'name': self.name,
            'data_json': self.data_json,
            'results': self.results,
            'data_json_validation_errors': self.data_json_validation_errors,
            'actions': self.final_results['actions'],
            'validation_errors': self.final_results['validation_errors'],
            'action_warnings': self.final_results['action_warnings'],
            'action_errors': self.final_results['action_errors']
        }
        return data

    def render_template(self, template_path, save=True):
        context = self.get_json_data()

        f = open(template_path)
        template = Template(f.read())
        html = template.render(**context)
        if save:
            self.save_report(html=html, report_path=config.get_html_report_path())

        return html

    def save_report(self, html, report_path):
        f = open(report_path, 'w')
        f.write(html)
        f.close()
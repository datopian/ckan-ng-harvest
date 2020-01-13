from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.datajson.harvester import DataJSON
from harvesters.logs import logger
import csv
import json
from harvesters import config


# search each data.json source and analyze them
cpa = CKANPortalAPI(base_url='https://catalog.data.gov')

# write results as CSV
csvfile = open('harvest_datasets_datagov_analysis.csv', 'w')
fieldnames = ['url', 'title', 'error', 'source_type', 'frequency',
              'validator_schema', 'config', 'collections', 'child_datasets',
              'download_ok', 'parsed_ok', 'validate_ok',
              'schema_version', 'total_dataset',
              'total_resources', 'dataset_types', 'resource_types']

writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
writer.writeheader()
final_results = []
colections_ids = set()
c = 0
urls = []
with_configs = 0
with_config_filters = 0
with_config_defaults = 0

for results in cpa.search_harvest_packages(harvest_type='harvest',
                                           method='GET'
                                           #,source_type='datajson'
                                          ):
    for local_harvest_source in results:

        url = local_harvest_source['url']
        if url in urls:
            logger.error('------------------\n   ALREADY READED\n------------------')
            continue
        else:
            urls.append(url)

        c += 1
        name = local_harvest_source.get('name', 'UNNAMED')
        hspath = config.get_harvest_sources_path(hs_name=name)
        f = open(hspath, 'w')
        f.write(json.dumps(local_harvest_source, indent=2))
        f.close()
        logger.info(f'{hspath} saved')

        # check for config.filters and config.defaults
        config_str = local_harvest_source.get('config', '{}')
        configs = json.loads(config_str)
        if configs != {}:
            with_configs += 1
        filters = configs.get('filters', None)
        if filters is not None:
            with_config_filters += 1
        defaults = configs.get('defaults', None)
        if defaults is not None:
            with_config_defaults += 1
        # default is federal v 1.1
        validator_schema = configs.get('validator_schema', None)
        if validator_schema is None:
            validator_schema = local_harvest_source.get('validator_schema', None)

        title = local_harvest_source['title']
        source_type = local_harvest_source['source_type']
        frequency = local_harvest_source.get('frequency', None)

        # for final_results
        result = {'url': url,
                  'title': title,
                  'validator_schema': validator_schema,
                  'config': configs,
                  'source_type': source_type,
                  'frequency': frequency,
                  'error': '',
                  'child_datasets': 0,
                  'collections': 0}

        if validator_schema is None:
            validator_schema = 'federal-v1.1'

        logger.info(f' +++ [[{source_type}]] Reading source {title} ({c}) from {url}')

        if source_type != 'datajson':
            result['schema_version'] = ''
            result['total_dataset'] = -1
            result['total_resources'] = -1
            result['child_datasets'] = -1
            result['collections'] = -1
            result['dataset_types'] = ''
            result['resource_types'] = ''
        else:
            dj = DataJSON()
            dj.url = url
            try:
                dj.fetch()
                ret = True
            except Exception as e:
                ret = False

            result['download_ok'] = ret
            if not ret:
                min_error = dj.errors[0][:70]
                result['error'] = 'Download error (truncated): {} ...'.format(min_error)
                logger.error(' +++++++++++ ERROR')
                logger.error(result['error'])
                writer.writerow(result)
                continue

            ret = dj.validate(validator_schema=validator_schema)
            result['validate_ok'] = ret
            if not ret:
                min_error = dj.errors[0][:70]
                result['error'] = 'Validation error (truncated): {} ...'.format(min_error)
                logger.error(' +++++++++++ ERROR')
                logger.error(result['error'])
                continue

            dj.post_fetch()
            result['schema_version'] = dj.schema_version
            result['total_dataset'] = len(dj.datasets)

            total_resources = 0
            # analyze types
            dataset_types = {}
            resource_types = {}
            for dataset in dj.datasets:
                if 'isPartOf' in dataset:
                    result['child_datasets'] += 1
                    colections_ids.add(dataset['isPartOf'])
                    result['collections'] = len(colections_ids)

                # check for type @type: dcat:Dataset
                dataset_type = dataset['@type'] if '@type' in dataset else 'unknown'
                if dataset_type not in dataset_types:
                    dataset_types[dataset_type] = 0
                dataset_types[dataset_type] += 1

                resources = dataset['distribution'] if 'distribution' in dataset else []
                if type(resources) == dict:
                    resources = [resources]
                elif type(resources) == list:
                    pass
                else:
                    result['error'] = 'unknown Distribution: {}'.format(dataset['distribution'])
                    resources = []

                total_resources += len(resources)

                for resource in resources:
                    resource_type = resource['@type'] if '@type' in resource else 'unknown'
                    if resource_type not in resource_types:
                        resource_types[resource_type] = 0
                    resource_types[resource_type] += 1

            result['total_resources'] = total_resources
            result['dataset_types'] = dataset_types
            result['resource_types'] = resource_types

            logger.info('*******************************')
            logger.info(f'RESULT: {result}')
            logger.info('*******************************')
        final_results.append(result)

        writer.writerow(result)

csvfile.close()

schemas = {}
for result in final_results:
    if result['validator_schema'] not in  schemas:
        schemas[result['validator_schema']] = 0
    schemas[result['validator_schema']] += 1

print('----------------------------------------------')
print('Finish {} sources'.format(len(final_results)))
print(f' - whit config: {with_configs}')
print(f'   - whit config filters: {with_config_filters}')
print(f'   - whit config defaults: {with_config_defaults}')
print(f'   - schemas: {schemas}')
print('----------------------------------------------')

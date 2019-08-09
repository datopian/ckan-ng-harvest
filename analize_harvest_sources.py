from libs.data_gov_api import CKANPortalAPI
from libs.data_json import DataJSON
from logs import logger
import csv


# search each data.json source and analyze them
cpa = CKANPortalAPI(base_url='https://catalog.data.gov')

# write results as CSV
csvfile = open('harvest_datasets_datagov_analysis.csv', 'w')
fieldnames = ['url', 'title', 'error', 'source_type', 'frequency',
              'collections', 'child_datasets',
              'download_ok',
              # 'download_error',
              'parsed_ok',
              # 'parsed_error',
              'validate_ok',
              # 'validate_error',
              'schema_version', 'total_dataset',
              'total_resources', 'dataset_types', 'resource_types']

writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
writer.writeheader()
results = []
colections_ids = set()
c = 0
urls = []

for results in cpa.search_harvest_packages(harvest_type='harvest', source_type='datajson'):
    for local_harvest_source in results:

        url = local_harvest_source['url']
        if url in urls:
            logger.error('------------------\n   ALREADY READED\n------------------')
            continue
        else:
            urls.append(url)

        c += 1

        title = local_harvest_source['title']
        source_type = local_harvest_source['source_type']
        frequency = local_harvest_source.get('frequency', None)

        # for final results
        result = {'url': url,
                  'title': title,
                  'source_type': source_type,
                  'frequency': frequency,
                  'error': '',
                  'child_datasets': 0,
                  'collections': 0}

        logger.info(f'Reading source  {title} ({c}) from {url}')

        dj = DataJSON()
        dj.url = url
        ret, error = dj.download_data_json()
        result['download_ok'] = ret
        if not ret:
            result['error'] = 'Download error (truncated): {} ...'.format(error[:70])
            logger.error(' +++++++++++ ERROR')
            logger.error(result['error'])
            writer.writerow(result)
            continue

        ret, error = dj.load_data_json()
        result['parsed_ok'] = ret
        if not ret:
            result['error'] = 'Parsing error (truncated): {} ...'.format(error[:70])
            logger.error(' +++++++++++ ERROR')
            logger.error(result['error'])
            writer.writerow(result)
            continue

        ret, errors = dj.validate_json()
        result['validate_ok'] = ret
        if not ret:
            result['error'] = 'Validation error (truncated): {} ...'.format(errors[0][:70])
            logger.error(' +++++++++++ ERROR')
            logger.error(result['error'])
            # some resources could be harvested with validation errors, continue by now

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
                resources = [resource]
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
        results.append(result)

        writer.writerow(result)

csvfile.close()
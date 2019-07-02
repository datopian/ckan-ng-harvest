import json
import requests
import logging
logger = logging.getLogger(__name__)

class CKANPortalAPI:
    """ API and data from data.gov 
        API SPECS: https://docs.ckan.org/en/latest/api/index.html """
    
    version = '0.01-alpha'
    user_agent = 'ckan-portal-filter'
    package_list_url = '/api/3/action/package_list'  # redirect to package_search (?)
    package_search_url = '/api/3/action/package_search'  # iterate with start and rows GET params
    package_list = None  

    def __init__(self, base_url='https://catalog.data.gov'):  # default data.gov
        self.base_url = base_url

    def get_request_headers(self):
        headers = {'User-Agent': f'{self.user_agent} {self.version}'}
        return headers

    def search_packages(self, rows=1000, harvest_source_id=None):
        """ search packages
            "rows" is the page size.
            You could search for an specific harvest_source_id """
        
        start = 0
        sort = "metadata_modified desc"

        url = '{}{}'.format(self.base_url, self.package_search_url)
        page = 0
        #TODO check for a real paginated version
        while url:
            page += 1

            params = {'start': start, 'rows': rows}  # , 'sort': sort}
            if harvest_source_id is not None:
                params['q'] = f'harvest_source_id:{harvest_source_id}'
                
            logger.debug(f'Searching {url} PAGE:{page} start:{start}, rows:{rows} with params: {params}')
            
            headers = self.get_request_headers()
            try:
                req = requests.get(url, params=params, headers=headers)
            except Exception as e:
                error = 'ERROR Donwloading package list: {} [{}]'.format(url, e)
                raise ValueError('Failed to get package list at {}'.format(url))
            
            content = req.content
            try:
                json_content = json.loads(content)  # check for encoding errors
            except Exception as e:
                error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
                raise ValueError(error)
            
            if not json_content['success']:
                error = 'API response failed: {}'.format(json_content.get('error', None))
                raise ValueError(error)
            
            result = json_content['result']
            count_results = result['count']
            sort_results = result['sort']
            facet_results = result['facets']
            results = result['results']
            real_results_count = len(results)
            logger.debug(f'{real_results_count} results')

            if real_results_count == 0:
                url = None
            else:
                start += rows
                
            yield(results)


if __name__ == '__main__':

    print('testing data')

    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('api.log')
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.setLevel(logging.DEBUG)

    cpa = CKANPortalAPI()
    resources = 0
    harvest_source_ids = ['de90314a-7c7d-4aff-bd84-87b134bba13d',  # Treasury JSON
                            '50104281-92a3-4534-9d38-141bc82276c5',  # NYC JSON
                            'afb32af7-87ba-4f27-ae5c-f0d4d0e039dc'  # CFPB JSON
                            ]
    
    for harvest_source_id in harvest_source_ids:
        page = 0
        for packages in cpa.search_packages(harvest_source_id=harvest_source_id):
            page += 1
            for package in packages:
                pkg_resources = len(package['resources'])
                resources += pkg_resources

            if len(packages) > 0:
                f = open(f'tmp_results_{harvest_source_id}_{page}.json', 'w')
                f.write(json.dumps(packages, indent=2))
                f.close()

            print('{} total resources'.format(resources))
    

from ckanapi import RemoteCKAN
import json
import requests
import logging
logger = logging.getLogger(__name__)

class CKANPortalAPI:
    """ API and data from data.gov 
        API SPECS: https://docs.ckan.org/en/latest/api/index.html """
    
    version = '0.02-alpha'
    user_agent = 'ckan-portal-filter'
    remote_ckan = None

    def __init__(self, remote_url='https://catalog.data.gov'):  # default data.gov
        self.remote_url = remote_url
        self.remote_ckan = RemoteCKAN(remote_url, user_agent=self.get_user_agent())

    def get_user_agent(self):
        return f'{self.user_agent} {self.version}'
    
    def search_packages(self, start=0, rows=1000, harvest_source_id=None):
        """ search packages
            "rows" is the page size.
            You could search for an specific harvest_source_id """

        
        search_params = {'rows': rows, 'start': start}
        if harvest_source_id is not None:
            search_params['q'] = f'harvest_source_id:{harvest_source_id}'
            
        logger.info(f'search with params: {search_params}')
        self.last_search_params = search_params
        results = self.remote_ckan.action.package_search(**search_params)
        # no funciona results = self.remote_ckan.action.package_search(rows=5)
        
        return results


if __name__ == '__main__':

    print('testing data')

    c_handler = logging.StreamHandler()
    f_handler = logging.FileHandler('api.log')
    logger.addHandler(c_handler)
    logger.addHandler(f_handler)
    logger.setLevel(logging.DEBUG)

    cpa = CKANPortalAPI()
    resources = 0
    harvest_source_id = 'de90314a-7c7d-4aff-bd84-87b134bba13d'  # Treasury JSON
    harvest_source_id = '50104281-92a3-4534-9d38-141bc82276c5'  # NYC JSON
    harvest_source_id = 'afb32af7-87ba-4f27-ae5c-f0d4d0e039dc'  # CFPB JSON
    
    results = cpa.search_packages(start=100, rows=5, harvest_source_id=harvest_source_id)
    print(results)

    f = open('tmp_results.json', 'w')
    f.write(json.dumps(results, indent=2))
    f.close()

    packages = 0
    resources = 0
    has_harvest_source_id = 0
    harvest_source_ids = set()

    errors = []
    for result in results['results']:
        packages += 1
        for resource in result['resources']:
            resources += 1
        this_harvest_source_id = None
        for extra in result['extras']:
            if extra['key'] == 'harvest_source_id':
                has_harvest_source_id += 1
                harvest_source_ids.add(extra['value'])
                if this_harvest_source_id is None:
                    this_harvest_source_id = extra['value']
                else:
                    if this_harvest_source_id != extra['value']:
                        error = 'Multi harvest_source_id:Package ID:{} {} {}'.format(result['id'], this_harvest_source_id, extra['value'])
                        errors.append(error)

    harvest_source_ids_count = len(harvest_source_ids)
    print(f'{packages} packages, {resources} resources {has_harvest_source_id} has harvest source. {harvest_source_ids_count} different ids')
    print(cpa.last_search_params)
    print('Errors: {}'.format(len(errors)))
    for error in errors:
        print(f'error: {error}')
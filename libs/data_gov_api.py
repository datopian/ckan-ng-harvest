import json
import requests
import logging
logger = logging.getLogger(__name__)
import os
from datapackage import Package, Resource
from slugify import slugify
import base64


class CKANPortalAPI:
    """ API and data from data.gov
        API SPECS: https://docs.ckan.org/en/latest/api/index.html """

    version = '0.01-alpha'
    user_agent = 'ckan-portal-filter'
    package_list_url = '/api/3/action/package_list'  # redirect to package_search (?)
    package_search_url = '/api/3/action/package_search'  # iterate with start and rows GET params
    package_create_url = '/api/3/action/package_create'
    # search for harvest sources
    package_search_harvested_sources_url = '/api/3/action/package_search?q=%28type:harvest%29&rows=1000'  # all the sources in a CKAN instance (959 results in data.gov)
    package_search_harvested_datajson_sources_url = '/api/3/action/package_search?q=%28type:harvest%20source_type:datajson%29&rows=1000'  # just the data.json harvest sources in a CKAN instance (144 results in data.gov)
    package_list = []
    total_packages = 0

    def __init__(self, base_url='https://catalog.data.gov'):  # default data.gov
        self.base_url = base_url

    def get_request_headers(self):
        headers = {'User-Agent': f'{self.user_agent} {self.version}'}
        return headers

    def search_harvest_packages(self, rows=1000, harvest_source_id=None,  # just one harvest source
                                                    harvest_type=None,  # harvest for harvest sources
                                                    source_type=None):  # datajson for
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
            elif harvest_type is not None:
                if source_type is not None:
                    params['q'] = f'(type:{harvest_type} source_type:{source_type})'
                else:
                    params['q'] = f'(type:{harvest_type})'



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
            self.total_packages += real_results_count
            logger.debug(f'{real_results_count} results')

            if real_results_count == 0:
                url = None
            else:
                start += rows
                self.package_list += results
                yield(results)

    def get_all_packages(self, harvest_source_id=None,  # just one harvest source
                                harvest_type=None,  # 'harvest' for harvest sources
                                source_type=None):
        self.package_list = []
        self.total_pages = 0
        for packages in self.search_harvest_packages(harvest_source_id=harvest_source_id,
                                                    harvest_type=harvest_type,
                                                    source_type=source_type):

            self.total_pages += 1

    def read_local_packages(self, path):
        if not os.path.isfile(path):
            return False, "File not exists"
        packages_file = open(path, 'r')
        try:
            self.package_list = json.load(packages_file)
        except Exception as e:
            return False, "Error parsin json: {}".format(e)
        return True, None

    def count_resources(self):
        """ read all datasets and count resources """
        total = 0
        for dataset in self.package_list:
            resources = dataset.get('resources', [])
            total += len(resources)
        return total

    def remove_duplicated_identifiers(self):
        unique_identifiers = []
        self.duplicates = []

        for dataset in self.package_list:
            idf = dataset['id']
            if idf not in unique_identifiers:
                unique_identifiers.append(idf)
            else:
                self.duplicates.append(idf)
                self.package_list.remove(dataset)

        return self.duplicates

    def save_packages_list(self, path):
        dmp = json.dumps(self.package_list, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()


    def create_package_from_data_json(self, dictt):
        """ transform a data.json dataset/package to a CKAN one
            ############
            # check how to map fields: https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L444
            # check the parser: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/parse_datajson.py#L5
            ############
            Analyze gather vs import stages
            https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L112

            https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L394



            data.json sample:
                {
                "accessLevel": "public",
                "bureauCode": ["018:001"],
                "license": "https://project-open-data.cio.gov/unknown-license",
                "identifier": "10.5439/1027273",
                "contactPoint": {"hasEmail": "mailto:adc@arm.gov", "fn": "ARM Data Center"},
                "description": "No description found",
                "programCode": ["018:001"],
                "distribution": [
                {
                    "accessURL": "http://www.archive.arm.gov/discovery/#v/results/s/s::aerich2nf1turn",
                    "format": "cdf"
                }
                ],
                "modified": "2019-06-27 12:41:27",
                "publisher": {
                "name": "Atmospheric Radiation Measurement Data Center",
                "subOrganizationOf": {
                    "name": "DOE Biological and Environmental Research (BER)",
                    "subOrganizationOf": {
                    "name": "DOE Office of Science user facilities",
                    "subOrganizationOf": {
                        "name": "U.S. Government"
                    }
                    }
                }
                },
                "@type": "dcat:Dataset",
                "keyword": ["ARM", "AERI Noise Filtered"],
                "title": "AERI ch. 2 radiance data, with uncorrelated random error (noise) filtered out"
            }
        """
        pass

    def create_package(self, package):
        """ POST to CKAN API to create a new package/dataset
            https://docs.ckan.org/en/2.8/api/#ckan.logic.action.create.package_create

            name (string) – the name of the new dataset, must be between 2 and 100 characters long and contain only lowercase alphanumeric characters, - and _, e.g. 'warandpeace'
            title (string) – the title of the dataset (optional, default: same as name)
            private (bool) – If True creates a private dataset
            author (string) – the name of the dataset’s author (optional)
            author_email (string) – the email address of the dataset’s author (optional)
            maintainer (string) – the name of the dataset’s maintainer (optional)
            maintainer_email (string) – the email address of the dataset’s maintainer (optional)
            license_id (license id string) – the id of the dataset’s license, see license_list() for available values (optional)
            notes (string) – a description of the dataset (optional)
            url (string) – a URL for the dataset’s source (optional)
            version (string, no longer than 100 characters) – (optional)
            state (string) – the current state of the dataset, e.g. 'active' or 'deleted', only active datasets show up in search results and other lists of datasets, this parameter will be ignored if you are not authorized to change the state of the dataset (optional, default: 'active')
            type (string) – the type of the dataset (optional), IDatasetForm plugins associate themselves with different dataset types and provide custom dataset handling behaviour for these types
            resources (list of resource dictionaries) – the dataset’s resources, see resource_create() for the format of resource dictionaries (optional)
            tags (list of tag dictionaries) – the dataset’s tags, see tag_create() for the format of tag dictionaries (optional)
            extras (list of dataset extra dictionaries) – the dataset’s extras (optional), extras are arbitrary (key: value) metadata items that can be added to datasets, each extra dictionary should have keys 'key' (a string), 'value' (a string)
            relationships_as_object (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            relationships_as_subject (list of relationship dictionaries) – see package_relationship_create() for the format of relationship dictionaries (optional)
            groups (list of dictionaries) – the groups to which the dataset belongs (optional), each group dictionary should have one or more of the following keys which identify an existing group: 'id' (the id of the group, string), or 'name' (the name of the group, string), to see which groups exist call group_list()
            owner_org (string) – the id of the dataset’s owning organization, see organization_list() or organization_list_for_user() for available values. This parameter can be made optional if the config option ckan.auth.create_unowned_dataset is set to True.

        """
        url = '{}{}'.format(self.base_url, self.package_create_url)

        # created_package = response_dict['result']

    def save_datasets_as_data_packages(self, folder_path):
        """ save each dataset source as _datapackage_ """
        for dataset in self.package_list:
            package = Package()

            #TODO check this, I'm learning datapackages
            resource = Resource({'data': dataset})
            resource.infer()
            identifier = dataset['id']
            bytes_identifier = identifier.encode('utf-8')
            encoded = base64.b64encode(bytes_identifier)
            encoded_identifier = str(encoded, "utf-8")

            resource_path = os.path.join(folder_path, f'resource_ckan_api_{encoded_identifier}.json')
            if not resource.valid:
                raise Exception('Invalid resource')

            resource.save(resource_path)

            package.add_resource(descriptor=resource.descriptor)
            package_path = os.path.join(folder_path, f'pkg_ckan_api_{encoded_identifier}.zip')
            package.save(target=package_path)


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
        for packages in cpa.search_harvest_packages(harvest_source_id=harvest_source_id):
            page += 1
            for package in packages:
                pkg_resources = len(package['resources'])
                resources += pkg_resources

            if len(packages) > 0:
                f = open(f'data/ckan_api_tmp_results_{harvest_source_id}_{page}.json', 'w')
                f.write(json.dumps(packages, indent=2))
                f.close()

            print('{} total resources'.format(resources))


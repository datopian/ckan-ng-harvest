"""
process Data JSON files
    check the schema definition: https://project-open-data.cio.gov/v1.1/schema/catalog.json
    validate: maybe with this https://github.com/Julian/jsonschema
"""
import requests
import jsonschema as jss
import json
import os
from datapackage import Package, Resource
from slugify import slugify


class JSONSchema:
    """ a JSON Schema definition for validating data.json files """
    json_content = None  # schema content 
    valid_schemas = {  # schemas we know
                "https://project-open-data.cio.gov/v1.1/schema": '1.1',
                }

    def __init__(self, url):
        self.url = url  # URL of de schema definition. e.g. https://project-open-data.cio.gov/v1.1/schema/catalog.json
        try:
            req = requests.get(self.url)
        except Exception as e:
            error = 'ERROR Donwloading schema: {} [{}]'.format(self.url, e)
            raise ValueError('Failed to get schema definition at {}'.format(url))
        
        content = req.content
        try:
            self.json_content = json.loads(content)  # check for encoding errors
        except Exception as e:
            error = 'ERROR parsing JSON data: {} [{}]'.format(content, e)
            raise ValueError(error)



class DataJSON:
    """ a data.json file for read and validation """
    url = None  # URL of de data.json file
    
    raw_data_json = None  # raw downloaded text
    data_json = None  # JSON readed from data.json file

    headers = None
    datasets = []  # all datasets described in data.json
    validation_errors = []
    duplicates = []  # list of datasets with the same identifier

    def download_data_json(self, timeout=30):
        """ download de data.json file """
        if self.url is None:
            return False, "No URL defined"
        
        try:
            req = requests.get(self.url, timeout=timeout)
        except Exception as e:
            error = 'ERROR Donwloading data: {} [{}]'.format(self.url, e)
            return False, error
        
        if req.status_code >= 400:
            error = '{} HTTP error: {}'.format(self.url, req.status_code)
            return False, error
            
        self.raw_data_json = req.content
        return True, None
    
    def read_local_data_json(self, data_json_path):
        if not os.path.isfile(data_json_path):
            return False, "File not exists"
        data_json_file = open(data_json_path, 'r')
        self.raw_data_json = data_json_file.read()
        return True, None
    
    def load_data_json(self):
        """ load as a JSON object """
        try:
            self.data_json = json.loads(self.raw_data_json)  # check for encoding errors
        except Exception as e:
            error = 'ERROR parsing JSON data: {}'.format(e)
            return False, error
    
        return True, None

    def validate_json(self):
        errors = []  # to return list of validation errors
        
        if self.data_json is None:
            return False, 'No data json available'
        
        if not self.data_json.get('describedBy', False):
            return False, 'Missing describedBy KEY'
            
        schema_definition_url = self.data_json['describedBy']
        self.schema = JSONSchema(url=schema_definition_url)
        ok, schema_errors = self.validate_schema()
        if not ok:
            errors += schema_errors
        
        # validate with jsonschema lib
        # many data.json are not extrictly valid, we use as if they are

        #TODO check and re-use a ckanext-datajson validator: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py
        
        try:
            jss.validate(instance=self.data_json, schema=self.schema.json_content)
        except Exception as e:
            error = "Error validating JsonSchema: {}".format(e)
            errors.append(error)
        
        #read datasets by now, even in error
        self.datasets = self.data_json['dataset']
        self.validation_errors = errors
        if len(errors) > 0:
            return False, errors
        else:
            return True, None
    
    def validate_schema(self):
        """ validate using jsonschema lib """

        #TODO check how ckanext-datajson uses jsonschema. One example (there are more) https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L368

        # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L120
        errors = []
        
        # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L137
        schema_value = self.data_json.get('conformsTo', '')
        if schema_value not in self.schema.valid_schemas.keys():
            errors.append(f'Error reading json schema value. "{schema_value}" is not known schema')
        schema_version = self.schema.valid_schemas.get(schema_value, '1.0')

        # list of needed catalog values  # https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L152
        catalog_fields = ['@context', '@id', 'conformsTo', 'describedBy']
        self.catalog_extras = dict(('catalog_'+k, v) for (k, v) in self.schema.json_content.items() if k in catalog_fields)

        return len(errors) == 0, errors

    def remove_duplicated_identifiers(self):
        unique_identifiers = []
        
        for dataset in self.datasets:
            idf = dataset['identifier']
            if idf not in unique_identifiers:
                unique_identifiers.append(idf)
            else:
                self.duplicates.append(idf)
                self.datasets.remove(dataset)
                
        return self.duplicates
    
    def count_resources(self):
        """ read all datasets and count resources """
        total = 0
        for dataset in self.datasets:
            distribution = dataset.get('distribution', [])
            total += len(distribution)
        return total
    
    def save_data_json(self, path):
        """ save the source data.json file """
        dmp = json.dumps(self.data_json, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_validation_errors(self, path):
        dmp = json.dumps(self.validation_errors, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_duplicates(self, path):
        dmp = json.dumps(self.duplicates, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()

    def save_datasets_as_data_packages(self, folder_path):
        """ save each dataset from a data.json source as _datapackage_ """
        for dataset in self.datasets:
            package = Package()
            
            #TODO check this, I'm learning datapackages
            resource = Resource({'data': dataset})
            resource.infer()  #adds "name": "inline"

            #FIXME identifier uses incompables characthers as paths (e.g. /).
            # could exist duplicates paths from different resources
            # use BASE64 or hashes
            idf = slugify(dataset['identifier'])  
            
            resource_path = os.path.join(folder_path, f'resource_data_json_{idf}.json')
            if not resource.valid:
                raise Exception('Invalid resource')
            
            resource.save(resource_path) 

            package.add_resource(descriptor=resource.descriptor)
            package_path = os.path.join(folder_path, f'pkg_data_json_{idf}.zip')
            package.save(target=package_path)

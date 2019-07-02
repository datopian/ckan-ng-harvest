"""
process Data JSON files
    check the schema definition: https://project-open-data.cio.gov/v1.1/schema/catalog.json
    validate: maybe with this https://github.com/Julian/jsonschema
"""
import requests
import jsonschema as jss
import json

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

    datasets = []  # all datasets described in data.json

    def download_data_json(self, timeout=30):
        """ download de data.json file """
        if self.url is None:
            return False, "No URL defined"
        
        try:
            req = requests.get(self.url, timeout=timeout)
        except Exception as e:
            error = 'ERROR Donwloading data: {} [{}]'.format(self.url, e)
            return False, error
            
        self.raw_data_json = req.content
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
        schema_definition_url = self.data_json['describedBy']
        self.schema = JSONSchema(url=schema_definition_url)
        ok, schema_errors = self.validate_schema()
        if not ok:
            errors += schema_errors
        
        # validate with jsonschema lib
        # many data.json are not extrictly valid, we use as if they
        is_valid = True
        try:
            jss.validate(instance=self.data_json, schema=self.schema.json_content)
        except Exception as e:
            error = "Error validating JsonSchema: {}".format(e)
            errors.append(error)
        
        #read datasets by now, even in error
        self.datasets = self.data_json['dataset']

        is_valid = len(errors) == 0
        
        if not is_valid:
            return False, errors
        return True, None
    
    def validate_schema(self):

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

    def normalize_datasets(self):
        """ Transfor data.json datasets in a standar CKAN-compatible OUT """
        ret = {'datasets': []}
        
        for dataset in self.datasets:
            normalized_dataset = {'title': dataset['title'],
                    'description': dataset['description']}
            ret['datasets'].append(normalized_dataset)
        return ret

    def save_datasets(self, path):
        """ save the data package json file. Normalize the data """

        dmp = json.dumps(self.normalize_datasets(), indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
    
    def save_data_json(self, path):
        """ save the source data.json file """
        dmp = json.dumps(self.data_json, indent=2)
        f = open(path, 'w')
        f.write(dmp)
        f.close()
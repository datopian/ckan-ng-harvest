"""
CSW stands for Catalog Service for the Web

Real Life cases (from catalog.data.gov):

Alaska LCC CSW Server: http://metadata.arcticlcc.org/csw
NC OneMap CSW: http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
USACE Geospatial CSW: http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
2017_arealm: https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/
GeoNode State CSW: http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief
OpenTopography CSW: https://portal.opentopography.org/geoportal/csw
"""
import requests
from slugify import slugify
from urllib.parse import urlparse, urlencode, urlunparse
from owslib.csw import CatalogueServiceWeb, namespaces
from owslib.ows import ExceptionReport

class CSWSource:
    """ A CSW Harvest Source """

    csw = None
    csw_info = {}

    errors = []
    datasets = []  # all datasets included
    validation_errors = []
    duplicates = []  # list of datasets with the same identifier

    def __init__(self, url):
        self.url = url

    def get_cleaned_url(self):
        # remove all URL params
        parts = urlparse(self.url)
        return urlunparse((parts.scheme, parts.netloc, parts.path, None, None, None))

    def connect_csw(self, timeout=120):
        # connect to csw source
        try:
            self.csw = CatalogueServiceWeb(self.url, timeout=timeout)
        except Exception as e:
            error = f'Error connection CSW: {e}'
            self.errors.append(error)
            return False
        return True

    def get_records(self, page=10):

        # TODO get filters fom harvest source
        # https://github.com/GSA/ckanext-spatial/blob/datagov/ckanext/spatial/harvesters/csw.py#L90
        cql = None

        # output schema
        # outputschema: the outputSchema (default is 'http://www.opengis.net/cat/csw/2.0.2')
        # "csw" at GeoDataGovGeoportalHarvester
        # "gmd" at CSWHarvester
        # outputschema = 'gmd'  # https://github.com/geopython/OWSLib/blob/master/owslib/csw.py#L551
        outputschema = 'csw'

        startposition = 0
        kwa = {
            "constraints": [],
            "typenames": 'csw:Record',
            "esn": 'brief',  # esn: the ElementSetName 'full', 'brief' or 'summary' (default is 'full')
            "startposition": startposition,
            "maxrecords": page,
            "outputschema": namespaces[outputschema],
            "cql": cql,
            }

        matches = 0
        self.csw_info['records'] = []

        while True:
            try:
                self.csw.getrecords2(**kwa)
            except Exception as e:
                error = f'Error getting records(2): {e}'
                self.errors.append(error)
                break
            if self.csw.exceptionreport:
                exceptions = self.csw.exceptionreport.exceptions
                error = 'Error getting records: {}'.format(exceptions)
                self.errors.append(error)
                # raise Exception(error)
                break

            if matches == 0:
                matches = self.csw.results['matches']

            records = self.csw.records.items()

            for record in records:
                key, csw_record = record
                value = self._xmd(csw_record)
                # value = csw_record_to_dict(csw_record)
                self.csw_info['records'].append(value)
                yield value

            if len(records) == 0:
                break

            startposition += page
            if startposition > matches:
                break

            kwa["startposition"] = startposition

    def _xmd(self, obj):
        # Dictize an object
        # https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/lib/csw_client.py#L28
        md = {}
        for attr in [x for x in dir(obj) if not x.startswith("_")]:
            val = getattr(obj, attr)
            if not val:
                pass
            elif callable(val):
                pass
            elif isinstance(val, str):
                md[attr] = val
            elif isinstance(val, int):
                md[attr] = val
            elif isinstance(val, list):
                md[attr] = val
            else:
                md[attr] = self._xmd(val)
        return md

    def get_record(self, identifier, esn='full'):
        #  Get Full record info
        try:
            records = self.csw.getrecordbyid([identifier], outputschema=namespaces['csw'])
        except ExceptionReport as e:
            self.errors.append(f'Error getting record {e}')
            # 'Invalid parameter value: locator=outputSchema' is an XML error
            return None

        csw_record = self.csw.records[identifier]
        # dict_csw_record = csw_record_to_dict(csw_record)
        dict_csw_record = self._xmd(csw_record)

        found = False
        for record in self.csw_info['records']:
            if record['identifier'] == identifier:
                record = dict_csw_record
                record['FULL'] = True
                found = True
        if not found:
            record = None
            self.errors.append(f'Identifier not found {identifier}')
        return record

    def get_local_record(self, identifier):
        # iterate internal list of records and return one
        for record in self.csw_info['records']:
            if record['identifier'] == identifier:
                return record
        return None


    def read_csw_info(self):
        # read some info about csw and put it in an internal dict
        csw_info = {}
        if self.csw is None:
            self.connect_csw()

        service = self.csw
        # Check each service instance conforms to OWSLib interface
        service.alias = 'CSW'
        csw_info['version'] = service.version
        csw_info['identification'] = {}  # service.identification
        csw_info['identification']['type'] = service.identification.type
        csw_info['identification']['version'] = service.identification.version
        csw_info['identification']['title'] = service.identification.title
        csw_info['identification']['abstract'] = service.identification.abstract
        csw_info['identification']['keywords'] = service.identification.keywords
        csw_info['identification']['accessconstraints'] = service.identification.accessconstraints
        csw_info['identification']['fees'] = service.identification.fees

        csw_info['provider'] = {}
        csw_info['provider']['name'] = service.provider.name
        csw_info['provider']['url'] = service.provider.url
        ctc = service.provider.contact
        contact = {'name': ctc.name,
                   'organization': ctc.organization,
                   'site': ctc.site,
                   'instructions': ctc.instructions,
                   'email': ctc.email,
                   'country': ctc.country}
        csw_info['provider']['contact'] = contact

        csw_info['operations'] = []
        for op in service.operations:
            methods = op.methods
            for method in methods:
                if type(method) == dict:
                    constraints = []
                    for k, v in method.items():
                        if k == 'constraints':
                            for c in v:
                                mc = {'name': c.name, 'values': c.values}
                                constraints.append(mc)
                                method['constraints'] = constraints

            operation = {'name': op.name,
                         'formatOptions': op.formatOptions,
                         'methods': methods}
            csw_info['operations'].append(operation)

        self.csw_info = csw_info
        return csw_info

    def get_original_url(self, harvest_id=None):
        # take the URL and add required params
        parts = urlparse(self.url)
        # urlparse('http://www.cwi.nl:80/%7Eguido/Python.html?q=90&p=881')
        # ParseResult(scheme='http', netloc='www.cwi.nl:80', path='/%7Eguido/Python.html', params='', query='q=90&p=881', fragment='')

        params = {
            'SERVICE': 'CSW',
            'VERSION': '2.0.2',
            'REQUEST': 'GetRecordById',
            'OUTPUTSCHEMA': 'http://www.isotc211.org/2005/gmd',
            'OUTPUTFORMAT': 'application/xml',
        }
        if harvest_id is not None:
            params['ID'] = harvest_id

        url = urlunparse((
            parts.scheme,
            parts.netloc,
            parts.path,
            None,
            urlencode(params),
            None
        ))

        return url

    def validate(self):
        errors = []  # to return list of validation errors
        # return False, errors

        return True, None

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
            pass  # TODO
        return total

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


def csw_record_to_dict(csw_record):
    # first test to underestand CSW Records
    ret = {
        'rdf': csw_record.rdf,
        'identifier': csw_record.identifier,
        'identifiers': csw_record.identifiers,
        'type': csw_record.type,
        'title': csw_record.title,
        'alternative': csw_record.alternative,
        'ispartof': csw_record.ispartof,
        'abstract': csw_record.abstract,
        'date': csw_record.date,
        'created': csw_record.created,
        'issued': csw_record.issued,
        'relation': csw_record.relation,
        'temporal': csw_record.temporal,
        'uris': csw_record.uris,
        'references': csw_record.references,
        'modified': csw_record.modified,
        'creator': csw_record.creator,
        'publisher': csw_record.publisher,
        'coverage': csw_record.coverage,
        'contributor': csw_record.contributor,
        'language': csw_record.language,
        'source': csw_record.source,
        'rightsholder': csw_record.rightsholder,
        'accessrights': csw_record.accessrights,
        'license': csw_record.license,
        'format': csw_record.format,
        'subjects': csw_record.subjects,
        'rights': csw_record.rights,
        'spatial': csw_record.spatial
        }
    return ret
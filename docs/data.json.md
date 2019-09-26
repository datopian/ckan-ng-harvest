# Data.json files 

_data.json_ files are in [json-ld](https://json-ld.org/) format ([wikipedia](https://en.wikipedia.org/wiki/JSON-LD)).  

Python implementations are:
 - [PyLD](https://github.com/digitalbazaar/pyld).
 - [rdflib-jsonld](https://github.com/RDFLib/rdflib-jsonld) (older).

The [ckanext-DCAT](https://github.com/ckan/ckanext-dcat) uses the [rdflib](https://rdflib.readthedocs.io/en/stable/).  

The json-ld files are like RDF serialized files. [DCAT](https://www.w3.org/TR/vocab-dcat/) is the _Data Catalog Vocabulary_: an RDF vocabulary designed to facilitate interoperability between data catalogs published on the Web.  

Data.json is like json with DCAT vocabulary. [See the fields context](https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld) we use.  

In the GSA fork of datajson extension we [only expect](https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L138) and analyze the _project-open-data_ [schema](https://project-open-data.cio.gov/v1.1/schema/catalog.json) (from CIO).  

At ckanext-datajson exists the [datajsonvalidator.py](https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/datajsonvalidator.py) file for check this resource.
# Harvester Next Generation for CKAN

## Install

```
pip install ckan-harvester
```
### Use data.json sources

```python
from harvester.data_json import DataJSON
dj = DataJSON()
dj.url = 'https://data.iowa.gov/data.json'
ret, error = dj.download_data_json()
print(ret, error)
# True None

ret, error = dj.load_data_json()
print(ret, error)
# True None

ret, errors = dj.validate_json()
print(ret, errors)
# False ['Error validating JsonSchema: \'bureauCode\' is a required property ...

# full dict with the source
print(dj.data_json)
"""
{
	'@context': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
	'@id': 'https://data.iowa.gov/data.json',
	'@type': 'dcat:Catalog',
	'conformsTo': 'https://project-open-data.cio.gov/v1.1/schema',
	'describedBy': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json',
	'dataset': [{
		'accessLevel': 'public',
		'landingPage': 'https://data.iowa.gov/d/23jk-3uwr',
		'issued': '2017-01-30',
		'@type': 'dcat:Dataset',

        ... 
"""
# just headers
print(dj.headers)

"""
{
'@context': 'https://project-open-data.cio.gov/v1.1/schema/catalog.jsonld',
'@id': 'https://data.iowa.gov/data.json',
'@type': 'dcat:Catalog',
'conformsTo': 'https://project-open-data.cio.gov/v1.1/schema',
'describedBy': 'https://project-open-data.cio.gov/v1.1/schema/catalog.json',
}
"""

for dataset in dj.datasets:
    print(dataset['title'])

Impaired Streams 2014
2009-2010 Iowa Public School District Boundaries
2015 - 2016 Iowa Public School District Boundaries
Impaired Streams 2010
Impaired Lakes 2014
2007-2008 Iowa Public School District Boundaries
Impaired Streams 2012
2011-2012 Iowa Public School District Boundaries
Active and Completed Watershed Projects - IDALS
2012-2013 Iowa Public School District Boundaries
2010-2011 Iowa Public School District Boundaries
2016-2017 Iowa Public School District Boundaries
2014 - 2015 Iowa Public School District Boundaries
Impaired Lakes 2008
2008-2009 Iowa Public School District Boundaries
2013-2014 Iowa Public School District Boundaries
Impaired Lakes 2010
Impaired Lakes 2012
Impaired Streams 2008

```


### Use CSW sources

```python
from harvester.csw import CSWSource
c = CSWSource(url='http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2')
csw.connect_csw()
 # True

csw_info = csw.read_csw_info()
print('CSW title: {}'.format(csw_info['identification']['title']))
 # CSW title: ArcGIS Server Geoportal Extension 10 - OGC CSW 2.0.2 ISO AP
```
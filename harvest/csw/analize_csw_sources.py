"""
Analize some CSW sources for test CSW library
"""

from slugify import slugify
from harvester import config
from harvester.csw import CSWSource
import csv
import json


url_services = [
            'http://metadata.arcticlcc.org/csw',
            'http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            'http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            'https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/',
            'http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief',
            'https://portal.opentopography.org/geoportal/csw'
        ]

outputschema = 'gmd'
# outputschema = 'csw'
for url in url_services:
    csw = CSWSource(url=url)
    connected = csw.connect_csw()
    if not connected:
        print(f'Fail to connect {csw.errors}')
        continue
    csw_info = csw.read_csw_info()

    name = slugify(csw_info['identification']['title'])
    print(f'CSW source ok: {name}')

    # get records
    print(f' - Gettings records from {name}')
    c = 0
    for record in csw.get_records(outputschema=outputschema):
        c += 1
        # add extra info about the first resources for test
        if c < 6:
            idf = record.get('identifier', None)
            if idf is None:
                print(f'NO IDENTIFIER!')
                continue
            print(f'idf full: {idf}')
            record = csw.get_record(identifier=idf, outputschema=outputschema)
            if record is None:
                print(csw.errors)
            print(record)

    try:
        as_str = json.dumps(csw.as_json(), indent=2)
    except Exception as e:
        as_str = f'Error serializing {csw.csw_info}: {e}'
        print(as_str)

    source_type = f'csw-{outputschema}'
    hspath = config.get_harvest_sources_data_path(source_type=source_type, name=name)

    f = open(hspath, 'w')
    f.write(as_str)
    f.close()

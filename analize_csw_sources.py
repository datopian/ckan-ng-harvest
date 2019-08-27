from libs.csw import CSWSource
from logs import logger
import csv
import json
import config
from slugify import slugify

url_services = [
            'http://metadata.arcticlcc.org/csw',
            'http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            'http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2',
            'https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/',
            'http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief',
            'https://portal.opentopography.org/geoportal/csw'
        ]

for url in url_services:
    csw = CSWSource(url=url)
    csw.url = csw.get_cleaned_url()
    if not csw.connect_csw():
        print(f'Fail to connect {csw.errors}')
        continue
    csw_info = csw.read_csw_info()
    try:
        as_str = json.dumps(csw_info, indent=2)
    except Exception as e:
        print(f'Error serializing {csw_info}: {e}')
        continue

    name = slugify(csw_info['identification']['title'])
    print(f'CSW source ok: {name}')
    hspath = config.get_harvest_sources_data_path(source_type='csw', name=name)

    # get records
    print(f' - Gettings records from {name}')
    for record in csw.get_records():
        print(record)

    as_str = json.dumps(csw.csw_info, indent=2)
    f = open(hspath, 'w')
    f.write(as_str)
    f.close()

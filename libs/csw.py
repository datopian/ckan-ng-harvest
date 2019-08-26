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


class CSWSource:
    """ A CSW Harvest Source """
    pass
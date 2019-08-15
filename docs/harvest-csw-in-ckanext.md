# Harvest CSW Sources
We use the [GeoDataGovCSWHarvester](https://github.com/GSA/ckanext-geodatagov/blob/f28f1751d23eac973d96394a2485ccfbd847135b/ckanext/geodatagov/harvesters/base.py#L165)


Inherit from:
 - [GeoDataGovHarvester](https://github.com/GSA/ckanext-geodatagov/blob/f28f1751d23eac973d96394a2485ccfbd847135b/ckanext/geodatagov/harvesters/base.py#L51): Common for different GIS harvesters.
 - [CSWHarvester](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L19).
    + Uses the [CswService](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/lib/csw_client.py#L64) for talk to CSW service. Finally uses [OWSLib](https://github.com/geopython/OWSLib)

### Diffing algorithm

Harvester [uses IDs](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L113) to detect changes.  


### Real Life cases

From catalog.data.gov:
 - Alaska LCC CSW Server: http://metadata.arcticlcc.org/csw
 - NC OneMap CSW: http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
 - USACE Geospatial CSW: http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
 - 2017_arealm: https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/
 - GeoNode State CSW: http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief
 - OpenTopography CSW: https://portal.opentopography.org/geoportal/csw
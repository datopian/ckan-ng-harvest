# Harvest CSW Sources
[CSW stands for Catalog Service for the Web](https://en.wikipedia.org/wiki/Catalogue_Service_for_the_Web) is a standard for exposing a catalogue of geospatial records in XML.  

We use the [GeoDataGovCSWHarvester](https://github.com/GSA/ckanext-geodatagov/blob/f28f1751d23eac973d96394a2485ccfbd847135b/ckanext/geodatagov/harvesters/base.py#L165)


Inherit from:
 - [GeoDataGovHarvester](https://github.com/GSA/ckanext-geodatagov/blob/f28f1751d23eac973d96394a2485ccfbd847135b/ckanext/geodatagov/harvesters/base.py#L51): Common to different GIS harvesters.
    + With _get_package_dict_ constructs a package_dict suitable to be passed to package_create or package_update.
    + Inherit from [SpatialHarvester](https://github.com/GSA/ckanext-spatial/blob/datagov/ckanext/spatial/harvesters/base.py#L112)
 - [CSWHarvester](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L19). Inherit _SpatialHarvester_.  
    + Uses the [CswService](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/lib/csw_client.py#L64) for talk to CSW service. Finally uses [OWSLib](https://github.com/geopython/OWSLib)


## Gather, Fetch, Import
First of all the [URL need to be parsed](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L35).  

[Gather](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L65): _CSWHarvester_ just connects to CSW service and save a guid list of packages at Harvest Objects.  

[Query](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L79) to local datasets harvested previously.  

Then iterate al external datasets (using a filter called _cql_).  

As diffing algorithm Harvester [uses IDs](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L113) to detect changes.  

[Fetch](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L145): Get one by one this datasets [by his identifier](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L167).  

[Save all XML response](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/csw.py#L181-L184) at the harverst object.  

Import [is done at SpatialHarvester](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L432). Take [the config for the harvest](https://github.com/GSA/ckanext-spatial/blob/2a25f8d60c31add77e155c4136f2c0d4e3b86385/ckanext/spatial/harvesters/base.py#L446).  



### Real Life cases

From catalog.data.gov:
 - Alaska LCC CSW Server: http://metadata.arcticlcc.org/csw
 - NC OneMap CSW: http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
 - USACE Geospatial CSW: http://metadata.usace.army.mil/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2
 - 2017_arealm: https://meta.geo.census.gov/data/existing/decennial/GEO/GPMB/TIGERline/TIGER2017/arealm/
 - GeoNode State CSW: http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief
 - OpenTopography CSW: https://portal.opentopography.org/geoportal/csw
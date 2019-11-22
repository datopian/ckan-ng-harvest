# Harvesting CSW resources

[CSW stands for Catalog Service for the Web](https://en.wikipedia.org/wiki/Catalogue_Service_for_the_Web) is a standard for exposing a catalogue of geospatial records in XML.  

You can use the _analyze_csw_sources_ command to leqrn more about the CSW sources.  

This harvest process includes:
 - Reads a CSW resource from an external source I want to harvest.
 - Validate and save these results.
 - Search the previous datasets harvested for that particular source
 - Compare both resources and list the differences.
 - Update the CKAN instance with these updates. 

This process is using:
 - [dataflows](https://github.com/datahq/dataflows) 
 - [datapackages](https://github.com/frictionlessdata/datapackage-py).  
 - [CKAN core data.json harvester](https://gitlab.com/datopian/ckan-ng-harvester-core).


Firsts trys
```
cd harvest/csw
python flow.py --url "http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief" --name geonode

Found c540e08e-015c-11e5-853f-22000b8e85d8 at http://geonode.state.gov/catalogue/csw
Found 11ea3390-1143-11e5-8c2b-22000b8e85d8 at http://geonode.state.gov/catalogue/csw
Found 373627ca-157a-11e7-bd9d-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found 3d03ffc2-9005-11e8-a6a3-12a62ff146a8 at http://geonode.state.gov/catalogue/csw
Found ef366484-157a-11e7-b627-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found c180c35c-157c-11e7-bd9d-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found 8cd1bad2-0159-11e5-9227-22000b8e85d8 at http://geonode.state.gov/catalogue/csw


python flow.py --url "https://portal.opentopography.org/geoportal/csw" --name opentopography

Found OTDS.082019.32616.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32616.2 at https://portal.opentopography.org/geoportal/csw
Found OT.072019.6339.1 at https://portal.opentopography.org/geoportal/csw
Found OT.082019.26912.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32749.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32633.2 at https://portal.opentopography.org/geoportal/csw

```
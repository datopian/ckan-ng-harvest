# Harvesting CSW resources

Firsts trys
```
python harvest/csw/flow.py --url "http://geonode.state.gov/catalogue/csw?service=CSW&version=2.0.2&request=GetRecords&typenames=csw:Record&elementsetname=brief" --name geonode

Found c540e08e-015c-11e5-853f-22000b8e85d8 at http://geonode.state.gov/catalogue/csw
Found 11ea3390-1143-11e5-8c2b-22000b8e85d8 at http://geonode.state.gov/catalogue/csw
Found 373627ca-157a-11e7-bd9d-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found 3d03ffc2-9005-11e8-a6a3-12a62ff146a8 at http://geonode.state.gov/catalogue/csw
Found ef366484-157a-11e7-b627-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found c180c35c-157c-11e7-bd9d-0a06d512a67a at http://geonode.state.gov/catalogue/csw
Found 8cd1bad2-0159-11e5-9227-22000b8e85d8 at http://geonode.state.gov/catalogue/csw


python harvest/csw/flow.py --url "https://portal.opentopography.org/geoportal/csw" --name opentopography

Found OTDS.082019.32616.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32616.2 at https://portal.opentopography.org/geoportal/csw
Found OT.072019.6339.1 at https://portal.opentopography.org/geoportal/csw
Found OT.082019.26912.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32749.1 at https://portal.opentopography.org/geoportal/csw
Found OTDS.082019.32633.2 at https://portal.opentopography.org/geoportal/csw

```
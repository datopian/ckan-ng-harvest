# Harvester Next Generation for CKAN

## Install

```
pip install ckan-harvester
```

### CSW sources

```python
from harvester.csw import CSWSource
c = CSWSource(url='http://data.nconemap.com/geoportal/csw?Request=GetCapabilities&Service=CSW&Version=2.0.2')
csw.connect_csw()
 # True

csw_info = csw.read_csw_info()
print('CSW title: {}'.format(csw_info['identification']['title']))
 # CSW title: ArcGIS Server Geoportal Extension 10 - OGC CSW 2.0.2 ISO AP
```
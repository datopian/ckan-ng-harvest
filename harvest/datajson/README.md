# Harvesting data.json resources

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Search the previous datasets harvested for that particular source
 - Compare both resources and list the differences.
 - Update the CKAN instance with these updates 

Current CKAN process: [using ckan extensions](harvest-in-ckanext.md).  

Process using [dataflows](https://github.com/datahq/dataflows) and [datapackages](https://github.com/frictionlessdata/datapackage-py).  
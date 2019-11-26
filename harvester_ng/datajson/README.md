# Harvesting data.json resources

The harvest process includes:
 - Read a [data.json](data.json.md) resource file from an external resource I want to harvest.
 - Validate and save these results.
 - Search the previous datasets harvested for that particular source
 - Compare both resources and list the differences.
 - Update the CKAN instance with these updates 

This process is using:
 - [dataflows](https://github.com/datahq/dataflows) 
 - [datapackages](https://github.com/frictionlessdata/datapackage-py).  
 - [CKAN core data.json harvester](https://gitlab.com/datopian/ckan-ng-harvester-core).

## Using harvester

```
python harvest_datajson.py \
  --name rrb \
  --url https://secure.rrb.gov/data.json \
  --harvest_source_id rrb-json \
  --ckan_owner_org_id rrb-gov \
  --catalog_url http://nginx:8080 \
  --ckan_api_key xxxxxxxxxxxxxxxxxxxxxx \
  --config '{"validator_schema" "federal-v1.1"}'
```

You can see the harvested datasets at you CKAN instance

![h0](/docs/imgs/harvested00.png)
![h1](/docs/imgs/harvested01.png)

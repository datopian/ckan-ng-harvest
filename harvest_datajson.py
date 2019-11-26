"""
Full harvest a data.json source
"""
import argparse
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination

parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json", required=True)
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)", required=True)
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API", required=True)
parser.add_argument("--ckan_owner_org_id", type=str, help="CKAN ORG ID", required=True)
parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API", required=True)
parser.add_argument("--ckan_api_key", type=str, help="API KEY working at CKAN instance", required=True)
parser.add_argument("--limit_datasets", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")
parser.add_argument("--config", type=str, help="Configuration of source, str-dict (validation_schema, default_groups, etc)")

# get Harvest Source config and set default schema for validation
args = parser.parse_args()

destination = CKANHarvestDestination(catalog_url=args.catalog_url,
                                     api_key=args.ckan_api_key,
                                     organization_id=args.ckan_owner_org_id,
                                     harvest_source_id=args.harvest_source_id)

hdj = HarvestDataJSON(name=args.name,
                      url=args.url,
                      destination=destination)
hdj.limit_datasets = args.limit_datasets
res = hdj.download()
hdj.save_download_results(flow_results=res)
res = hdj.compare()
hdj.save_compare_results(flow_results=res)
res = hdj.write_destination()
hdj.save_write_results(flow_results=res)
hdj.write_final_report()

"""
sample
python harvester_ng/datajson/harvest.py \
    --name dol-json \
    --url http://www.dol.gov/data.json \
    --harvest_source_id 59f68f20-9d44-4a3e-958e-46a5935ef591 \
    --ckan_owner_org_id 762a7be2-c2ed-4d10-bbac-05faca90b9e7 \
    --catalog_url http://nginx:8080 \
    --ckan_api_key 21bbafcf-f0d7-475c-8c98-e1baf25ba13e \
    --config {}

commands = [
    ['python3', 'flow.py', '--name', args.name, '--url', args.url, '--limit_dataset', str(args.limit_dataset), '--config', json.dumps(harverst_source_config)],
    ['python3', 'flow2.py', '--name', args.name, '--harvest_source_id', args.harvest_source_id, '--catalog_url', args.catalog_url],
    ['python3', 'flow3.py', '--name', args.name, '--ckan_owner_org_id', args.ckan_owner_org_id, '--catalog_url', args.catalog_url, '--ckan_api_key', args.ckan_api_key]
]
"""
"""
Full harvest a data.json source
"""
import argparse
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination
from harvester_ng.logs import logger


parser = argparse.ArgumentParser()
parser.add_argument("--url", type=str, help="URL of the data.json", required=True)
parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)", required=True)
parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API", required=True)
parser.add_argument("--ckan_owner_org_id", type=str, help="CKAN ORG ID", required=True)
parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API", required=True)
parser.add_argument("--ckan_api_key", type=str, help="API KEY working at CKAN instance", required=True)
parser.add_argument("--limit_datasets", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")
parser.add_argument("--config", type=str, help="Configuration of source, str-dict (validation_schema, default_groups, etc)")

logger.info('Start a DataJSON harvest process')

# get Harvest Source config and set default schema for validation
args = parser.parse_args()

destination = CKANHarvestDestination(catalog_url=args.catalog_url,
                                     api_key=args.ckan_api_key,
                                     organization_id=args.ckan_owner_org_id,
                                     harvest_source_id=args.harvest_source_id)

logger.info(f'Harvest source: CKAN {args.url}')
logger.info(f'Harvest Destination: CKAN {args.catalog_url}')

hdj = HarvestDataJSON(name=args.name,
                      url=args.url,
                      destination=destination)
hdj.limit_datasets = args.limit_datasets

logger.info('Downloading from source')
res = hdj.download()
hdj.save_download_results(flow_results=res)
logger.info('Comparing data')
res = hdj.compare()
hdj.save_compare_results(flow_results=res)
logger.info('Writting results at destination')
res = hdj.write_destination()
hdj.save_write_results(flow_results=res)
logger.info('Writting final report')
hdj.write_final_report()

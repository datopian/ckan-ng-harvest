"""
Full harvest a data.json source
"""
import argparse
from harvester_ng.source_datajson import HarvestDataJSON
from harvester_ng.harvest_destination import CKANHarvestDestination
from harvester_ng.logs import logger


def harvest_datajson(url, name, harvest_source_id, ckan_owner_org_id,
                     catalog_url, ckan_api_key, limit_datasets=0, 
                     config={}):
    """ Harvest a DataJSON source 
    
    Args:
        url: URL of the data.json source
        name: Name we give to the source
        harvest_source_id: Source ID for filter CKAN API
        ckan_owner_org_id: Organization ID at CKAN destination
        catalog_url: URL for write CKAN API
        ckan_api_key: Valid API KEY at CKAN instance
        limit_datasets: Limit datasets to harvest on each source. Defualt=0 => no limit
        config: Configuration of source, str-dict (validation_schema, default_groups, etc)
        """
    logger.info('Start a DataJSON harvest process')

    destination = CKANHarvestDestination(catalog_url=catalog_url,
                                     api_key=ckan_api_key,
                                     organization_id=ckan_owner_org_id,
                                     harvest_source_id=harvest_source_id)

    logger.info(f'Harvest source: CKAN {url}')
    logger.info(f'Harvest Destination: CKAN {catalog_url}')

    hdj = HarvestDataJSON(name=name,
                        url=url,
                        destination=destination,
                        config=config)
    hdj.limit_datasets = limit_datasets

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
    logger.info('Harvest process finished')


if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("--url", type=str, help="URL of the data.json", required=True)
    parser.add_argument("--name", type=str, help="Name of the resource (for generate the containing folder)", required=True)
    parser.add_argument("--harvest_source_id", type=str, help="Source ID for filter CKAN API", required=True)
    parser.add_argument("--ckan_owner_org_id", type=str, help="Organization ID at CKAN destination", required=True)
    parser.add_argument("--catalog_url", type=str, help="URL for write CKAN API", required=True)
    parser.add_argument("--ckan_api_key", type=str, help="API KEY working at CKAN instance", required=True)
    parser.add_argument("--limit_datasets", type=int, default=0, help="Limit datasets to harvest on each source. Defualt=0 => no limit")
    parser.add_argument("--config", type=str, help="Configuration of source, str-dict (validation_schema, default_groups, etc)")

    # get Harvest Source config and set default schema for validation
    args = parser.parse_args()

    harvest(url=args.url,
            name=args.name,
            harvest_source_id=args.harvest_source_id,
            ckan_owner_org_id=args.ckan_owner_org_id,
            catalog_url=args.catalog_url,
            ckan_api_key=args.ckan_api_key,
            limit_datasets=args.limit_datasets,
            config=args.config)
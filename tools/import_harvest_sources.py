import os
import argparse
from harvester_adapters.ckan.api import CKANPortalAPI
from harvesters.logs import logger

parser = argparse.ArgumentParser()
parser.add_argument("--import_from_url", type=str, help="CKAN instance URL to import from")
parser.add_argument("--harvest_type", type=str, default='harvest', help="Dataset type for harvest is 'harvest'")
parser.add_argument("--source_type", type=str, default='datajson', help="Type of harvest source: datajson|csw|waf etc")
parser.add_argument("--method", type=str, default='GET', help="POST fails on CKAN 2.3, now is working")
parser.add_argument("--destination_url", type=str, help="CKAN instance URL to import to")
parser.add_argument("--destination_api_key", type=str, help="CKAN instance URL to import to")

args = parser.parse_args()

cpa = CKANPortalAPI(base_url=args.destination_url, api_key=args.destination_api_key)

total_sources = cpa.import_harvest_sources(catalog_url=args.import_from_url,
                                           method=args.method,
                                           on_duplicated='DELETE',
                                           harvest_type=args.harvest_type,
                                           source_type=args.source_type,
                                           delete_local_harvest_sources=False)

# search
total_searched = 0
for harvest_sources in cpa.search_harvest_packages(method='POST',
                                                   harvest_type=args.harvest_type,
                                                   source_type=args.source_type):
    for harvest_source in harvest_sources:
        total_searched += 1

print('----------------------------------------------')
print(f'Finished: {total_sources} sources')
print('----------------------------------------------')

assert total_sources == total_searched

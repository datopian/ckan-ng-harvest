from harvester.data_gov_api import CKANPortalAPI
import argparse
from settings import CKAN_BASE_URL, CKAN_API_KEY


parser = argparse.ArgumentParser()
parser.add_argument("--import_from_url", type=str, help="CKAN instance URL to imprt from")
parser.add_argument("--harvest_type", type=str, default='harvest', help="Dataset type for harvest is 'harvest'")
parser.add_argument("--source_type", type=str, default='datajson', help="Tipe of harvest source: datajson|csw|waf etc")
parser.add_argument("--method", type=str, default='GET', help="POST fails on CKAN 2.3, now is working")

args = parser.parse_args()

cpa = CKANPortalAPI(base_url=CKAN_BASE_URL, api_key=CKAN_API_KEY)

total_sources = cpa.import_harvest_sources(catalog_url=args.import_from_url,
                                           method=args.method,
                                           on_duplicated='DELETE',
                                           harvest_type=args.harvest_type,
                                           source_type=args.source_type,
                                           delete_local_harvest_sources=True)

# search
total_searched = 0
for harvest_sources in cpa.search_harvest_packages(method='POST',
                                                   harvest_type=args.harvest_type,
                                                   source_type=args.source_type):
    for harvest_source in harvest_sources:
        total_searched += 1

assert total_sources == total_searched

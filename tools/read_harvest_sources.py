from harvester_adapters.ckan.api import CKANPortalAPI
import argparse


parser = argparse.ArgumentParser()
parser.add_argument("--base_url", type=str, help="CKAN instance URL")
parser.add_argument("--harvest_type", type=str, default='harvest', help="Dataset type for harvest is 'harvest'")
parser.add_argument("--source_type", type=str, default='datajson', help="Tipe of harvest source: datajson|csw|waf etc")
parser.add_argument("--method", type=str, default='GET', help="POST fails on CKAN 2.3, now is working")

args = parser.parse_args()

cpa = CKANPortalAPI(base_url=args.base_url)

for harvest_sources in cpa.search_harvest_packages(method=args.method,
                                                   harvest_type=args.harvest_type,
                                                   source_type=args.source_type):
    for dataset in harvest_sources:
        print(f'Harvest source: {dataset}')

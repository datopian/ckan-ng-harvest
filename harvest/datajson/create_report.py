"""
Read ALL results files about the harvest process and write a report
"""
from harvester import config
import argparse
from harvester.harvested_source import HarvestedSource

parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generating the containing folder)")
args = parser.parse_args()

hs = HarvestedSource(name=args.name)

hs.process_results()
hs.render_template(save=True)

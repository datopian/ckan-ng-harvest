"""
Read ALL results files about the harvest process and write a report
"""
import json
import argparse
from harvesters import config
from tools.results.harvested_source import HarvestedSource


parser = argparse.ArgumentParser()
parser.add_argument("--name", type=str, help="Name of the resource (for generating the containing folder)")
args = parser.parse_args()

hs = HarvestedSource(name=args.name)

hs.process_results()

# write results
results = hs.get_json_data()
f = open(config.get_final_json_results_for_report_path(), 'w')
f.write(json.dumps(results, indent=2))
f.close()

hs.render_template(save=True)

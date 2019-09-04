"""
Read ALL results from ALL harvest process
"""

from harvester.harvested_source import HarvestedSources

hss = HarvestedSources()
hss.process_all()
"""
Read ALL results from ALL harvest process
"""

from harvesters.harvested_source import HarvestedSources

hss = HarvestedSources()
hss.process_all()
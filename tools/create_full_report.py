"""
Read ALL results from ALL harvest process
"""

from .results import HarvestedSources

hss = HarvestedSources()
hss.process_all()
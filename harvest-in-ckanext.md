# Actual Harvest process

The current harvest process lives in a group of CKAN extensions.
Some notes.  

#### Define parents and childs identifiers 

https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145


#### Define catalog_extras 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L153

#### Iterate ALL Harvest Objects at CKAN instance 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L164

and list which still exist  
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L174
 
create a separate list for which are parents  
        
#### Detect which was demoted and which was promoted to parent
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L179
error if one opackage is parent and child (identifier is part of both lists) 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L198

#### Detect new parents (this one who are parents in data.json but not before) 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L202

#### Delete all no longer in the remote catalog 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L322

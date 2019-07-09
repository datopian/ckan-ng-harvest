# Actual Harvest process

The current harvest process lives in a group of CKAN extensions.

## Some code notes

#### load_remote_catalog
Read a data.json URL.  
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_datajson.py#L21

#### Define parents and childs identifiers 

https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145


#### Define catalog_extras 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L153

#### Loop through the packages we've already imported from this source
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L164
        
#### Detect which was demoted and which was promoted to parent

https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L179  

error if one opackage is parent and child (identifier is part of both lists)  
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L198  

#### Detect new parents (this one who are parents in data.json but not before) 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L201

#### Delete all no longer in the remote catalog 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L322

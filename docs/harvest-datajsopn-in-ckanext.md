# Actual Harvest process

The current harvest process lives in a group of CKAN extensions.

## Data.json harvest

When you install datajson extension you will be able to validate data.json at http://CKAN-INSTANCE/pod/validate.  
You also can access CKAN harvest logs via the API: http://CKAN-INSTANCE/api/3/action/harvest_log_list.  
Alowwed parameters:
 - level (filter log records by level)
 - limit (used for pagination)
 - offset (used for pagination)

More help here: https://github.com/ckan/ckanext-harvest  

### Commands
All the harvesting processes starts with a command ([CkanCommand](https://github.com/ckan/ckanext-harvest/blob/master/ckanext/harvest/commands/harvester.py#L13)).  

_Source_ command: use the action _harvest_source_create_. [Code for creating harvest source](https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/commands/harvester.py#L281) the harvest source.  

_job_ command: use the action _harvest_job_create_. [Code for creating harvest job](https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/commands/harvester.py#L385).  

_run_ command: use the action _harvest_jobs_run_.  

Check all [models and tables](https://github.com/ckan/ckanext-harvest/blob/3a72337f1e619bf9ea3221037ca86615ec22ae2f/ckanext/harvest/model/__init__.py#L250) in harvester.  


### The data.json harvesting process

#### gather_stage. MAIN PART OF THE PROCESS

The process starts in [gather_stage function](https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L112).  

#### load_remote_catalog
Read a data.json URL: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_datajson.py#L21  
Define parents and childs identifiers: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L145  
Define catalog_extras: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L153  
Loop through the packages we've already imported from this source: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L164  
Detect which was demoted and which was promoted to parent: https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L179  
error if one opackage is parent and child (identifier is part of both lists): https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L198  
Detect new parents (this one who are parents in data.json but not before): https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L201  

#### Save harvest object to process later: 
https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L318  

#### Delete all no longer in the remote catalog: 
https://github.com/GSA/ckanext-datajson/blob/datagov/ckanext/datajson/harvester_base.py#L322  

#### Fetch stage
Is not needed for data.json

#### Import stage
Here we write the datasets inside CKAN instance.  

The gather stage adds extra data in harvest objects. [Import stage uses them](https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L402-L415).  

Loads the [defaults values](https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L435). Maybe we need default values for datasets from the same source (maybe organization?).  
If the schema version this function move all to lowercase. TODO: analyze if we need.  
Each datasets was validated trought _validate_dataset and Draft4Validator.


Finally [creates or saves new datasets](https://github.com/GSA/ckanext-datajson/blob/07ca20e0b6dc1898f4ca034c1e073e0c27de2015/ckanext/datajson/harvester_base.py#L676).

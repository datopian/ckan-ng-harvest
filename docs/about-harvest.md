# Data.gov 

Data.gov publishes it's own resources AND thounsands of resources from another OpenData portal in USA (Dept. of Energy, Educatation, etc).  

### To Start

We have two main resources: 

 - a list of datasets in each Open Data portal. Usually it's a [data.json file](data.json.md). Sometimes it is another type of resource (we are not working with now). By now we just work with data.json sources. 
 - a previous harvested list of these datasets in data.gov data.json file. We get this list by filtering the CKAN API with the harvest_source_id.

### Goal
1. So, in the harvest process we need to merge these two resources detecting new, modified or deleted datasets.
2. Create a list of changes since last harvest
3. Use CKAN API to write changes (CRUD) using list of updates
4. Notify appropriate parties of results / errors etc

### Requirements

* Need hooks to route notifications to proper recipients

## Use case

* 2500 datasets/data packages in data.json from dept of energy
* 2400 previous results from CKAN API from dept. of energy

We want to store the harvested datasets as data packages and generate required outputs

### Analysis

Using the file system is good because it allows us to break the process into discrete parts using dataflows

For example, we could use cron (or Airflow), paralelize processing, etc

Data.gov has millions of datasets, in order to harvest them we will need to break the harvest into multiple steps and it will be necessary to save the intermediate steps or the harvested datasets to disk

#### Desired outputs from harvest process

* datapackage.json files for all resources harvested
* validation errors
* notifications
* diff file with difference between current and last harvest
* save stats about updates
  * save private dataset with stats about process (analytics)

#### NG Harvester System Definition

v1 components:

* lib for processing data packages (exists, https://frictionlessdata.io/docs/using-data-packages-in-python/)
* lib for processing data.json files (maybe does not exist)
* lib for processing ckan api results (exists and it's called _ckanapi_)
* dataflows to coordinate these processing steps
* maybe some thin wrapper to coordinate dataflows 
* validation / results
  * store results
* notification system
  * configure notification recipients
  * specification for configuration (location, format, permissions)
  * specification for storing outputs (data packages?, validation results, etc)

Optional

* [optional - nice to have] analytics 
* [optional - nice to have] expose a documented interface / API so that users can run the harvest, see the results, etc


### Actions / Next steps

- [ ] Update epics / Create tickets in Gitlab
- [ ] Research existing tooling for working with datapackages
  - [ ] Do data package tools work for our needs


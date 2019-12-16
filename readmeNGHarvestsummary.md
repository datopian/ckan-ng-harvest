# Description

NG Harvester summary of work completed and to be done

### Benefits of NG Harvester

 - Easier to manage harvesters and data pipelines
 - Easier to debug when things go wrong (logs not written to CKAN logs, logs are understandable to a non-tech user
 - Easier to maintain

Tech: Deculpled from CKAN so run on a seperate machine, simplified prod code


# Acceptance criteria 
 - [ ] All metadata sources are being effectively harvested using the NG Harvester / Data Pipelines

# Check-list of work
 - [x] NG Harvester architecture (working version v1.0 working)
 - [x] Airflow infrastructure (using airflow as the scheduler) 
 - [x] Deployed in the sandbox
 - [x] NG Harvester for one source (ie datajson)
 - [ ] Harvester admin/config page moved to scheming extension (remove harvester and sub-modules) [4 days work]
 - [ ] Functional but not pretty UI front-end status page [1 day work]
 - [ ] Define API to UI [1 day work]
    - [ ] The [Airflow API](https://airflow.apache.org/docs/stable/api.html) is in an experimental status. We think that [Airflow rest API plugin](https://github.com/teamclairvoyant/airflow-rest-api-plugin) could be useful.It's a plugin for Apache Airflow that exposes rest end points for the [Command Line Interfaces](http://airflow.apache.org/cli.html). Also we could use [serve_logs](http://airflow.apache.org/cli.html#serve_logs) from Airflow which add the [/log URL]( https://github.com/apache/airflow/blob/master/airflow/bin/cli.py#L1137). We need to define this and implement. This API could be used by a single UI or a CKAN extension. We already installed Airflow API plugin. We need to define which endpoint we are going to use (maybe half a day) and then define the UI to start using it.
    - [ ] API should only return data for admins. We need an auth solution here. [3 to 5 days] (Need private so people don't run harvesters but in there might be creds in logs) 
 - [ ] Making logs readable for non-tech user [1 day for MVP and then iterated based on feedback]
    - [ ] Analyze the actual logs and its HTML version. Show it inside some UI we define. Analyze this and improve.

### Repos for each repo?
Are we going to use one repo for all harvester or one repo for each?  > Yes, different repo and have a registry to have auto installed. 

Future work: 
Additional reporting, improved design of UI, new sources? Create a proper backend for config and db outstide CKAN. 




# Check-list of sources types
 - [x] data.json
 - [ ] csw #103: Worked with the previous harvester version. We need to update it after our refactor. We need maybe 2 days for this.
 - [ ] arcgis #104: Not started. We need to analyze it. Difficult to estimate. We are going to need at least 3 days for create this new harvester.
 - [ ] WAF #105: Not started. We need to analyze it. Difficult to estimate. We are going to need at least 3 days for create this new harvester.
 - [ ] WAF Collections #109: We do some work with this at legacy geodatagov ext. Seems easy to migrate to new harvester. Maybe 2 or 3 days.
 - [ ] DOC Harvester #106: Not started. We need to analyze it. Difficult to estimate. We are going to need at least 3 days for create this new harvester.
 - [ ] geo portal #107: Not started. We need to analyze it. Difficult to estimate. We are going to need at least 3 days for create this new harvester.
 - [ ] Z3950 #108: Not started. We need to analyze it. Difficult to estimate. We are going to need at least 3 days for create this new harvester.
 
 
 
## WIP Job stories: 

When a Harvest Run is initiated by an (Expert) Harvest Operator they want to see it is running and be notified of application and (meta)data errors as soon as possible, especially “halts” so that they can debug and re-run
 
If there are a lot of (data) errors I want to examine these in a system that allows me to analyse / view easily (i.e. don’t have my page crash as it tries to load 100k error messages)
I don’t want to receive 100k error emails … 
 
When a scheduled Harvest Run happens as a Harvest Operator (Sysadmin), I want to be notified afterwards (with a report?) if something went wrong, so that I can fix it. 
 
When I need to report to my colleagues about the Harvest




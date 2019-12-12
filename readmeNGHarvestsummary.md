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
 - [x] NG Harvester architecture (working v1.0 working)
 - [x] Airflow infrastructure (using airflow as the scheduler) 
 - [x] Deployed in the sandbox
 - [x] NG Harvester for one source
 - [ ] Functional but not pretty UI front-end work (1/2 days work)
 - [ ] Define API to UI (estimate: needs to be investigated) - would prefer in CKAN 
 - [ ] Making logs readable for non-tech user (1 day for v1.0)
 
 Just using airflow? 

What's the work required to be using the new harvesters?  

Future work: 
Additional reporting, improved design of UI, new sources?




# Check-list of sources types
 - [x] data.json
 - [ ] csw #103
 - [ ] arcgis #104
 - [ ] WAF #105
 - [ ] WAF Collections #109
 - [ ] DOC Harvester #106
 - [ ] geo portal #107
 - [ ] Z3950 #108
 
 
 
 
## WIP Job stories: 

When a Harvest Run is initiated by an (Expert) Harvest Operator they want to see it is running and be notified of application and (meta)data errors as soon as possible, especially “halts” so that they can debug and re-run
 
If there are a lot of (data) errors I want to examine these in a system that allows me to analyse / view easily (i.e. don’t have my page crash as it tries to load 100k error messages)
I don’t want to receive 100k error emails … 
 
When a scheduled Harvest Run happens as a Harvest Operator (Sysadmin), I want to be notified afterwards (with a report?) if something went wrong, so that I can fix it. 
 
When I need to report to my colleagues about the Harvest




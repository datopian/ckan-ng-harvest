# Notes about logging harvest process

While we harvest a source we save different files:
![](https://i.imgur.com/Enwkpnb.png)
(this names are at a renaming process)

We save a file with data from harvested source, a file with the dataset at CKAN to compare, a file with the comparison results and _data packages_ used for internal process.

Finally we have a class (called _HarvestedSource_) that reads all the results and creates a global log file (in json format).  

This file includes data, results, and errors from all the other log files.

![](https://i.imgur.com/7z5jJL1.png)

Examples:
![](https://i.imgur.com/vDJ6ITr.png)

![](https://i.imgur.com/9mYeLtc.png)

![](https://i.imgur.com/49KCIMz.png)

We also includes a render function in order to show results as HTML (as an example of use).  

![](https://i.imgur.com/b6hADHL.png)

## Python Logger

We are using python logging library in general managed by a [main log file](https://gitlab.com/datopian/ckan-ng-harvest/blob/64_harvest_with_config/harvester_ng/logs.py).  

## Airflow logs

Airflow save all logs about each harvest process and is available for analyze.  

![](https://i.imgur.com/YH3gACh.png)

![](https://i.imgur.com/VIpo5bj.png)

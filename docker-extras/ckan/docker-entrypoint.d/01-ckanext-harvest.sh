#!/bin/bash

# echo "Install requirements ckanext-harvester"

# cd /srv/app/src_extensions
# git clone --single-branch --branch master https://github.com/ckan/ckanext-harvest.git
# cd ckanext-harvest
# pip install -r pip-requirements.txt

echo "Init ckanext-harvester"
paster --plugin=ckanext-harvest harvester initdb
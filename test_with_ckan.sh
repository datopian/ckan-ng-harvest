DEVHOME=$(pwd)
echo '***************************'
echo 'Test data.json'
cd "$DEVHOME/harvest/datajson/"
python -m pytest tests_with_ckan/
pwd

echo '***************************'
echo 'Test package'
cd "$DEVHOME/libs/"
pwd
python -m pytest tests_with_ckan/
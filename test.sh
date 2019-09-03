DEVHOME=$(pwd)
echo '***************************'
echo 'Test data.json'
cd "$DEVHOME/harvest/datajson/"
python -m pytest tests/

echo '***************************'
echo 'Test csw'
cd "$DEVHOME/harvest/csw/"
python -m pytest tests/

echo '***************************'
echo 'Test package'
cd "$DEVHOME/libs/"
python -m pytest tests/
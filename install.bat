@echo off
::START SCRIPT

echo -------------------------------------------------------------
echo "Install Libs"
echo -------------------------------------------------------------

pip install requests
pip install sphinx
pip install pandas
pip install pyodbc
pip install pymssql
pip install xlsxwriter
pip install openpyxl
pip install .


echo -------------------------------------------------------------
echo "Ready....."
echo -------------------------------------------------------------

pause
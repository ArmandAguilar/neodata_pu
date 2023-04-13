@echo off
::START SCRIPT

echo -------------------------------------------------------------
echo "Install Libs"
echo -------------------------------------------------------------

echo ".__   __.  _______   ______    _______       ___   .___________.    ___      "
echo "|  \ |  | |   ____| /  __  \  |       \     /   \  |           |   /   \     "
echo "|   \|  | |  |__   |  |  |  | |  .--.  |   /  ^  \ `---|  |----`  /  ^  \    "
echo "|  . `  | |   __|  |  |  |  | |  |  |  |  /  /_\  \    |  |      /  /_\  \   "
echo "|  |\   | |  |____ |  `--'  | |  '--'  | /  _____  \   |  |     /  _____  \  "
echo "|__| \__| |_______| \______/  |_______/ /__/     \__\  |__|    /__/     \__\ "
echo "-----------------------------------------------------------------------------"
echo
echo 


pip install requests
pip install pandas
pip install pyodbc
pip install pymssql
pip install xlsxwriter
pip install openpyxl
pip install .

:: Make the lib
python -m build

::Install the lib
pip install %CD%\dist\neodata_pu-1.0.1-py3-none-any.whl

:: Make the Folder
md "C:\NeodataReportesExcel"


echo -------------------------------------------------------------
echo "Ready....."
echo -------------------------------------------------------------

pause
@echo off
title Equipment Dashboard
python "C:\python_scripts\qlik_to_blob\main.py"
python "C:\python_scripts\equipment_dashboard\equipment_dashboard.py"
python "C:\python_scripts\heinz_prediction\read_files_to_df.py"

exit
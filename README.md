# COVID-19 Data ETL Pipeline

This repository contains two scripts. The script `load-hhs.py` is used to load, clean, and insert/update weekly hospital data from the US Department of Health and Human Services (HHS) about hospitals throughout the US into PostgreSQL table called `Hospitals` and `Averages`. The script `load-quality.py` is used to load, clean, and insert/update weekly hospital data from a hospital quality dataset from the Centers for Medicare and Medicaid Services (CMS) into PostgreSQL tables called `Hospitals` and `Statuses`. Before loading the data, it is important to first create PostgreSQL tables (named `Hospitals`, `Averages`, and `Statuses`) using the `CREATE TABLE` commands found in `table_schema.sql` file.

The script `load-hhs.py` should be used to load csv files from the `hhs_data` subdirectory, while the `load-quality.py` script should be used to load csv files from the `hospital_qualities` subdirectory. **Always run `load-hhs.py` script before the `load-quality` script as the data in `hospital_qualities` has a foriegn key constraint with a column in the `hhs_data` csv files**. 

The script `weekly-report.ipynb` is used to create a report of hospital metrics in the past week. This script should be ran after loading data with the above two scripts. After being executed, the `weekly-report.ipynb` will create an HTML file that the user can open to evaluate hospital analytics.

---

### Executing `load-hhs.py` script

You can run the `load-hhs.py` script by executing the following command in terminal:
```
python load-hhs.py [csv_file_name_from_hhs_data]
```
where  `[csv_file_name_from_hhs_data]` is the file name of the desired hhs csv file.

For example:
```
python load-hhs.py 2022-01-04-hhs-data.csv
```
command would insert data from `2022-01-04-hhs-data.csv` into the `Hospitals` and `Averages` tables in PostgreSQL.

---

### Executing `load-quality.py` script

You can run the `load-quality.py` script by excuting the following command in terminal:
```
python load-quality.py [YYYY-MM-DD] [csv_file_name_from_hospital_quality]
```
Here, the first argument `[YYYY-MM-DD]` is the date of this quality data in YYYY-mm-dd format, and the second argument `[csv_file_name_from_hospital_quality]` is again the file name of the desired csv file. 

For example:
```
python load-quality.py 2021-07-01 Hospital_General_Information-2021-07.csv
```
command would insert data from `Hospital_General_Information-2021-07.csv` into the `Statuses` table and update `county`, `type`, and `ownership` columns in the `Hospitals` table in PostgreSQL.

---

### Executing `weekly-report.ipynb` script

You can run the weekly-report.ipynb by executing the following two commands in terminal:
```
papermill weekly-report.ipynb [file name].ipynb -p week [YYYY-MM-DD]
jupyter nbconvert --no-input --to html [file name].ipynb
```
Here, the first argument `[file name]` is a name of your choosing for the output report, and `[YYYY-MM-DD]` is the data that analytics are being reported for. In the second line, `[file name]` is the same name as the fle name chosen in the first line.

For example:
```
papermill weekly-report.ipynb 2022-09-23-report.ipynb -p week 2022-09-23
jupyter nbconvert --no-input --to html 2022-09-23-report.ipynb
```
commands create an output file called 2022-09-23-report.html using 2022-09-23 as the reference date for analysis.

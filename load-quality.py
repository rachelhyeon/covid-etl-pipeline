import psycopg
import credentials
import pandas as pd
import numpy as np
import os
import sys


#  Update statement for hospitals
UPDATE_HOSPITAL = """
    UPDATE hospitals AS h
    SET (county, type, ownership) = (%(county)s, %(type)s, %(own)s)
    WHERE h.hospital_id = %(fac_id)s;
"""

# Insert statement for statuses
INSERT_STATUS = """
    INSERT INTO Statuses (
        hospital_id,
        report_date,
        emergency,
        overall_quality_rating
    )
    VALUES (
        %(fac_id)s,
        %(report_date)s,
        %(emergency)s,
        %(overall_quality_rating)s
    );
"""


def update_error(error, row, index):
    """Handle errors found when updating hospital details.

    Keyword arguments:
    error -- the error being handled
    row -- the row for which an error is occuring
    index -- the index of the row from the original data
    """
    print(error)
    row_transposed = row.to_frame().transpose()
    print("Insert failed for row %s:" % index)
    print(row_transposed)
    row_transposed.to_csv(
        "failed_rows_quality.csv", mode="a",
        header=not os.path.exists("failed_rows_quality.csv"),
        na_rep="NULL", index_label="row_index"
    )
    print("Appended failed row to failed_rows_quality.csv")


def status_exception(error, row, index):
    """Handle errors found  when inserting statuses into table.

    Keyword arguments:
    error -- the error being handled
    row -- the row for which an error is occuring
    index -- the index of the row from the original data
    """
    print(error)
    row_transposed = row.to_frame().transpose()
    print("Insert into Statuses failed for row %s:" % index)
    print(row_transposed)
    row_transposed.to_csv(
        "failed_rows_status.csv", mode="a",
        header=not os.path.exists("failed_rows_status.csv"),
        na_rep="NULL", index_label="row_index"
    )
    print("Appended failed row to failed_rows_statuses.csv")


def execute_hospital_update(cur, row):
    """Execute SQL update statements for hospitals.

    Keyword arguments:
    cur -- the cursor of the SQL connection
    row -- the row for which an error is occuring
    """
    # Save into one variable
    facility_id = row["Facility ID"]
    county = row["County Name"]
    hospital_type = row["Hospital Type"]
    own = row["Hospital Ownership"]
    # Update hospitals table
    cur.execute(
        UPDATE_HOSPITAL, {
            "county": county,
            "type": hospital_type,
            "own": own,
            "fac_id": facility_id
        }
    )


def execute_status_transaction(cur, row, report_date):
    """Execute SQL insert statements for statuses.

    Keyword arguments:
    cur -- the cursor of the SQL connection
    row -- the row for which an error is occuring
    report_date -- the date of the status entity
    """
    # Save into one variable
    facility_id = row["Facility ID"]
    emergency = row["Emergency Services"]
    overall_quality_rating = row["Hospital overall rating"]
    # Update hospitals table
    cur.execute(
        INSERT_STATUS, {
            "fac_id": facility_id,
            "report_date": report_date,
            "emergency": emergency,
            "overall_quality_rating": overall_quality_rating
        }
    )


def process_data(report_date, path):
    """Main function to process csv files and insert statuses
       into SQL tables as well as update hospitals.

    Keyword arguments:
    report_date -- the report_date of the csv file
    path -- the file path of the csv file being processed
    """
    hosp_qual = pd.read_csv("hospital_qualities/" + path)
    # Do data cleaning
    hosp_qual_cleaned = hosp_qual.replace({
        "Not Available": None,
        np.nan: None}
    )
    # Only select columns that you need
    hosp_qual_reduced = hosp_qual_cleaned[["Facility ID",
                                           "County Name",
                                           "Hospital Type",
                                           "Hospital Ownership"]]
    # Only select columns that you need for statuses table
    hosp_qual_status = hosp_qual_cleaned[["Facility ID",
                                          "Emergency Services",
                                          "Hospital overall rating"]]

    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
        user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()
    num_rows_updated = 0
    # How many rows you want to load
    n = len(hosp_qual)
    with conn.transaction():
        # Load data into SQL into hospitals table
        for i in range(n):
            try:
                # Create new savepoint inside the for loop
                with conn.transaction():
                    row = hosp_qual_reduced.iloc[i]
                    execute_hospital_update(cur, row)
            except Exception as e:
                update_error(e, row, i)
            else:
                num_rows_updated += 1
        num_rows_inserted_status = 0
        # Load data into SQL for statuses table
        for i in range(n):
            try:
                # Create new savepoint inside the for loop
                with conn.transaction():
                    row = hosp_qual_status.iloc[i]
                    execute_status_transaction(cur, row, report_date)
            except Exception as e:
                status_exception(e, row, i)
            else:
                num_rows_inserted_status += 1
        # Print summary of update
        print("%s rows were updated" % num_rows_updated)
        # Print summary of rows loaded to table
        print("%s rows were loaded to Statuses table"
              % num_rows_inserted_status)
    conn.commit()  # commit transaction
    conn.close()  # close connection


# Get report_date and path from command line
if len(sys.argv) > 1:
    report_date = sys.argv[1]
    path = sys.argv[2]
process_data(report_date, path)

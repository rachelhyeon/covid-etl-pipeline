import psycopg
import credentials
import pandas as pd
import numpy as np
import re
import os
import sys

# Insert statement for hospital
INSERT_HOSPITAL = """
    INSERT INTO hospitals AS h (
        hospital_id,
        hospital_name,
        state,
        address,
        city,
        zip,
        fips_code,
        lat,
        long,
        county,
        type,
        ownership
    )
    VALUES (
        %(id)s,
        %(name)s,
        %(state)s,
        %(address)s,
        %(city)s,
        %(zip)s,
        %(fips)s,
        CAST (%(lat)s AS DECIMAL),
        CAST (%(long)s AS DECIMAL),
        %(county)s,
        %(type)s,
        %(own)s
    )
    ON CONFLICT (hospital_id) DO UPDATE
    SET
        hospital_name = COALESCE(
            excluded.hospital_name, h.hospital_name
        ),
        state = COALESCE(excluded.state, h.state),
        address = COALESCE(excluded.address, h.address),
        city = COALESCE(excluded.city, h.city),
        zip = COALESCE(excluded.zip, h.zip),
        fips_code = COALESCE(
            excluded.fips_code, h.fips_code
        ),
        lat = COALESCE(excluded.lat, h.lat),
        long = COALESCE(excluded.long, h.long)
    WHERE excluded.hospital_name IS NOT NULL
        OR excluded.state IS NOT NULL
        OR excluded.address IS NOT NULL
        OR excluded.city IS NOT NULL
        OR excluded.zip IS NOT NULL
        OR excluded.fips_code IS NOT NULL
        OR excluded.lat IS NOT NULL
        OR excluded.long IS NOT NULL;
"""

# Insert statement for averages
INSERT_AVERAGE = """
    INSERT INTO averages (
        hospital_id,
        report_date,
        adult_hospital_beds_avg,
        pediatric_inpatient_beds_avg,
        adult_occupied_beds_coverage,
        pediatric_occupied_beds_avg,
        total_icu_beds_avg,
        icu_beds_used_avg,
        beds_used_covid_avg,
        staffed_adult_icu_covid_avg
    )
    VALUES (
        %(a)s,
        %(b)s,
        %(c)s,
        %(d)s,
        %(e)s,
        %(f)s,
        %(g)s,
        %(h)s,
        %(i)s,
        %(j)s
    );
"""


def subset_hospitals(data):
    """Extract relevant specification columns from hospitals.

    Keyword arguments:
    data -- the data being subsetted
    """
    return data[
        [
            "hospital_pk",
            "hospital_name",
            "state",
            "address",
            "city",
            "zip",
            "fips_code",
            "geocoded_hospital_address"]
        ]


def subset_averages(data):
    """Extract relevant numeric columns from hospitals.

    Keyword arguments:
    data -- the data being subsetted
    """
    return data[
        [
            "hospital_pk",
            "collection_week",
            "all_adult_hospital_beds_7_day_avg",
            "all_pediatric_inpatient_beds_7_day_avg",
            "all_adult_hospital_inpatient_bed_occupied_7_day_coverage",
            "all_pediatric_inpatient_bed_occupied_7_day_avg",
            "total_icu_beds_7_day_avg",
            "icu_beds_used_7_day_avg",
            "inpatient_beds_used_covid_7_day_avg",
            "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
        ]


def hospital_exception(error, row, index):
    """Handle errors found  when inserting hospital into table.

    Keyword arguments:
    error -- the error being handled
    row -- the row for which an error is occuring
    index -- the index of the row from the original data
    """
    print(error)
    row_transposed = row.to_frame().transpose()
    print("Insert into Hospitals failed for row %s:" % index)
    print(row_transposed)
    row_transposed.to_csv(
        "failed_rows_hospitals.csv", mode="a",
        header=not os.path.exists("failed_rows_hospitals.csv"),
        na_rep="NULL", index_label="row_index"
    )
    print("Appended failed row to failed_rows_hospitals.csv")


def average_exception(error, row, index):
    """Handle errors found  when inserting average into table.

    Keyword arguments:
    error -- the error being handled
    row -- the row for which an error is occuring
    index -- the index of the row from the original data
    """
    print(error)
    row_transposed = row.to_frame().transpose()
    print("Insert into Averages failed for row %s:" % index)
    print(row_transposed)
    row_transposed.to_csv(
        "failed_rows_averages.csv", mode="a",
        header=not os.path.exists("failed_rows_averages.csv"),
        na_rep="NULL", index_label="row_index"
    )
    print("Appended failed row to failed_rows_averages.csv")


def execute_hospital_transaction(cur, row):
    """Execute SQL insert statements for hospitals.

    Keyword arguments:
    cur -- the cursor of the SQL connection
    row -- the row for which an error is occuring
    """
    # Save into one variable
    hospital_pk = row["hospital_pk"]
    hospital_name = row["hospital_name"]
    state = row["state"]
    address = row["address"]
    city = row["city"]
    zip = str(row["zip"])
    fips_code = row["fips_code"]
    # Use regex to extract latitude and longitude
    point = row["geocoded_hospital_address"]
    points = re.findall(r'-?\d+\.\d+', str(point))
    if points:
        # if points list is not empty
        lat = points[1]
        longitude = points[0]
    else:
        lat = None
        longitude = None
    # select from hospital_pk where exists new hospital_pk
    cur.execute(
        INSERT_HOSPITAL, {
            "id": hospital_pk,
            "name": hospital_name,
            "state": state,
            "address": address,
            "city": city,
            "zip": zip,
            "fips": fips_code,
            "lat": lat,
            "long": longitude,
            "county": None,
            "type": None,
            "own": None
        }
    )


def execute_average_transaction(cur, row):
    """Execute SQL insert statements for averages.

    Keyword arguments:
    cur -- the cursor of the SQL connection
    row -- the row for which an error is occuring
    """
    # Save into one variable
    hospital_pk = row["hospital_pk"]
    coll_week = row["collection_week"]
    ad_beds_7davg = row["all_adult_hospital_beds_7_day_avg"]
    ped_beds_7davg = row["all_pediatric_inpatient_beds_7_day_avg"]
    ad_beds_oc_7dcov = row[
        "all_adult_hospital_inpatient_bed_occupied_7_day_coverage"]
    ped_beds_oc_7davg = row[
        "all_pediatric_inpatient_bed_occupied_7_day_avg"]
    tot_icu_beds_7davg = row["total_icu_beds_7_day_avg"]
    icu_beds_used_7davg = row["icu_beds_used_7_day_avg"]
    beds_covid_7davg = row["inpatient_beds_used_covid_7_day_avg"]
    staff_icu_ad_covid_7davg = row[
        "staffed_icu_adult_patients_confirmed_covid_7_day_avg"]
    cur.execute(
        INSERT_AVERAGE, {
            'a': hospital_pk,
            'b': coll_week,
            'c': ad_beds_7davg,
            'd': ped_beds_7davg,
            'e': ad_beds_oc_7dcov,
            'f': ped_beds_oc_7davg,
            'g': tot_icu_beds_7davg,
            'h': icu_beds_used_7davg,
            'i': beds_covid_7davg,
            'j': staff_icu_ad_covid_7davg
        }
    )


def process_data(path):
    """Main function to process csv files and insert hospitals
       and averages into SQL tables.

    Keyword arguments:
    path -- the file path of the csv file being processed
    """
    # Load data into Pandas data frame
    hhs_data = pd.read_csv("hhs_data/" + path)
    # Do data cleaning
    hhs_data_cleaned = hhs_data.replace({-999999: None, np.nan: None})
    # Only select columns that you need
    hhs_data_reduced = subset_hospitals(hhs_data_cleaned)
    hhs_data_averages = subset_averages(hhs_data_cleaned)
    # print(hhs_data["hospital_pk"].nunique())  # 4995 unique hospital_pk
    # This is the expected number of rows our final SQL table should have
    conn = psycopg.connect(
        host="sculptor.stat.cmu.edu", dbname=credentials.DB_USER,
        user=credentials.DB_USER, password=credentials.DB_PASSWORD
    )
    cur = conn.cursor()
    # How many rows you want to load
    n = len(hhs_data)
    with conn.transaction():
        num_rows_inserted_hosp = 0
        # Load data into SQL
        for i in range(n):
            try:
                # Create new savepoint inside the for loop
                with conn.transaction():
                    row = hhs_data_reduced.iloc[i]
                    execute_hospital_transaction(cur, row)
            except Exception as e:
                hospital_exception(e, row, i)
            else:
                num_rows_inserted_hosp += 1
        # Print summary of rows loaded to Hospitals table
        print(
            "%s rows were loaded to Hospitals table" % num_rows_inserted_hosp)

        num_rows_inserted_avg = 0
        for j in range(n):
            try:
                # Create new savepoint inside the for loop
                with conn.transaction():
                    row = hhs_data_averages.iloc[j]
                    execute_average_transaction(cur, row)
            except Exception as e:
                average_exception(e, row, j)
            else:
                num_rows_inserted_avg += 1
        # Print summary of rows loaded to table
        print("%s rows were loaded to Averages table" % num_rows_inserted_avg)
    conn.commit()  # commit transaction
    conn.close()  # close connectio


# Get path from command line
if len(sys.argv) > 1:
    path = sys.argv[1]
process_data(path)

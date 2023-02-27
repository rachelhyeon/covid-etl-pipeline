CREATE TABLE Hospitals (
hospital_id varchar PRIMARY KEY,
hospital_name varchar,
state char(2),
address varchar(255),
city text,
zip varchar(10),
fips_code varchar(5),
lat decimal CHECK (lat <= 90 AND lat >= -90),
long decimal CHECK (long <= 180 AND long >= -180),
county text,
type varchar,
ownership varchar
);

CREATE TABLE Averages (
average_id serial PRIMARY KEY NOT NULL,
hospital_id varchar REFERENCES hospitals (hospital_id),
report_date date CHECK (report_date <= CURRENT_DATE),
adult_hospital_beds_avg numeric CHECK (adult_hospital_beds_avg >= 0),
pediatric_inpatient_beds_avg numeric CHECK (pediatric_inpatient_beds_avg >= 0),
adult_occupied_beds_coverage numeric CHECK (adult_occupied_beds_coverage >=0),
pediatric_occupied_beds_avg numeric CHECK (pediatric_occupied_beds_avg >= 0),
total_icu_beds_avg numeric CHECK (total_icu_beds_avg >= 0),
icu_beds_used_avg numeric CHECK (icu_beds_used_avg >= 0),
beds_used_covid_avg numeric CHECK (beds_used_covid_avg >= 0),
staffed_adult_icu_covid_avg numeric CHECK (staffed_adult_icu_covid_avg  >= 0)
);

CREATE TABLE Statuses (
status_id serial PRIMARY KEY NOT NULL,
hospital_id varchar REFERENCES hospitals (hospital_id),
report_date date CHECK (report_date <= CURRENT_DATE),
emergency boolean DEFAULT false,
overall_quality_rating integer CHECK (overall_quality_rating >=0)
);

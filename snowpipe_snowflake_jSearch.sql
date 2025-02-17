CREATE DATABASE snowflake_database;
use snowflake_database;

CREATE OR REPLACE storage integration s3_init_job
    TYPE=EXTERNAL_STAGE
    STORAGE_PROVIDER=S3
    ENABLED=TRUE
    STORAGE_AWS_ROLE_ARN='arn:aws:iam::703671895332:role/job-search-load-role'
    STORAGE_ALLOWED_LOCATIONS=('s3://job-search-etl-aws-dag/transformed_data/')

DESC integration s3_init_job;

CREATE OR REPLACE TABLE job_searched(
    job_id STRING,
    job_title STRING,
    employer_name STRING,
    job_description STRING,
    job_publisher STRING,
    job_employment_type STRING,
    job_apply_link STRING,
    job_apply_is_direct BOOLEAN,
    job_is_remote BOOLEAN,
    job_posted_at STRING,
    job_city STRING,
    job_state STRING,
    job_country STRING,
    job_salary FLOAT,
    job_min_salary FLOAT,
    job_max_salary FLOAT,
    job_qualifications STRING,
    job_responsibilities STRING
);

CREATE OR REPLACE FILE FORMAT csv_file_format
TYPE = CSV
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
SKIP_HEADER = 1
ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE
NULL_IF = ('', 'NULL');

CREATE OR REPLACE STAGE loading_data_job
    URL='s3://job-search-etl-aws-dag/transformed_data/'
    STORAGE_INTEGRATION=s3_init_job
    FILE_FORMAT = csv_file_format

LIST @loading_data_job;


CREATE OR REPLACE PIPE job_pipe
auto_ingest=True
AS
COPY INTO job_searched
FROM @loading_data_job
FILE_FORMAT=(FORMAT_NAME=csv_file_format,ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE);

DESC pipe job_pipe;
SELECT SYSTEM$PIPE_STATUS('job_pipe');

SELECT * FROM job_searched;

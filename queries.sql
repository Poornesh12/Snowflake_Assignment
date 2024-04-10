-- Question 1: Create roles as per the below-mentioned hierarchy. Accountadmin already exists in Snowflake ( 10 ).

-- Create the admin, developer, and PII roles.
CREATE ROLE admin;
CREATE ROLE developer;
CREATE ROLE PII;

-- Grant roles to each other and to Accountadmin as per the hierarchy.
GRANT ROLE admin TO ROLE Accountadmin;
GRANT ROLE developer TO ROLE admin;
GRANT ROLE PII TO ROLE Accountadmin;

-- Grant necessary privileges to the admin role.
-- These privileges allow the admin role to perform administrative tasks.
GRANT CREATE DATABASE, CREATE WAREHOUSE, CREATE USER, CREATE ROLE, CREATE INTEGRATION ON ACCOUNT TO ROLE admin;

/************************************************************************************************************************************/

-- Question 2: Create an M-sized warehouse using the accountadmin role, name -> assignment_wh and use it for all the queries ( 5 ).

-- Create M-sized warehouse with specific configuration settings.
CREATE WAREHOUSE assignment_wh 
  WITH WAREHOUSE_SIZE = 'Medium' 
  MAX_CLUSTER_COUNT = 1 
  MIN_CLUSTER_COUNT = 1 
  AUTO_SUSPEND = 600 
  AUTO_RESUME = TRUE 
  INITIALLY_SUSPENDED = TRUE; 

-- Grant usage privilege to relevant roles for the newly created warehouse.
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE ACCOUNTADMIN;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE ADMIN;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE Developer;
GRANT USAGE ON WAREHOUSE assignment_wh TO ROLE PII;

/************************************************************************************************************************************/
-- Question 3: Switch to the admin role ( 5 ).

-- Switch the current session to the admin role for executing subsequent commands.
USE ROLE admin;

/************************************************************************************************************************************/
-- Question 4: Create a database assignment_db ( 5 )

-- Create a new database named 'assignment_db'.
CREATE DATABASE assignment_db;

/************************************************************************************************************************************/
-- Question 5: Create a schema my_schema ( 5 )

-- Switch to the ACCOUNTADMIN role to grant privileges.
USE ROLE ACCOUNTADMIN;

-- Grant the privilege to create schema on the assignment_db database to the admin role.
GRANT CREATE SCHEMA ON DATABASE assignment_db TO ROLE admin;

-- Switch back to the ADMIN role for schema creation.
USE ROLE ADMIN;

-- Create a new schema named 'my_schema' within the assignment_db database.
CREATE SCHEMA my_schema;

/************************************************************************************************************************************/
-- Granting proper privileges to each role.

-- Grant privileges for the admin role
USE ROLE ACCOUNTADMIN;
GRANT USAGE ON DATABASE assignment_db TO ROLE admin;
GRANT ALL PRIVILEGES ON SCHEMA my_schema TO ROLE admin;
GRANT CREATE TABLE, CREATE VIEW, CREATE PIPE, CREATE STAGE ON SCHEMA my_schema TO ROLE admin;

-- Grant privileges for the developer role
GRANT USAGE ON DATABASE assignment_db TO ROLE developer;
GRANT USAGE ON SCHEMA my_schema TO ROLE developer;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA my_schema TO ROLE developer;
GRANT CREATE TABLE, CREATE VIEW ON SCHEMA my_schema TO ROLE developer;

-- Grant privileges for the PII role
GRANT USAGE ON DATABASE assignment_db TO ROLE PII;
GRANT USAGE ON SCHEMA my_schema TO ROLE PII;
GRANT SELECT, INSERT, UPDATE, DELETE ON TABLE my_schema.people TO ROLE PII;

/************************************************************************************************************************************/

-- Question 6 to Question 8


-- Use the ADMIN role for these tasks.
USE ROLE ADMIN;

---------------------Data into internal stage------------------------

-- Create or use an existing internal stage and grant privileges to the admin role.
CREATE OR REPLACE STAGE your_internal_stage;
GRANT ALL PRIVILEGES ON STAGE your_internal_stage TO ROLE admin;

-- Put the file into the internal stage.
-- Assuming the file people-10000.csv is already present at the specified location.
PUT file:///Users/poornesh/Downloads/people-10000.csv @your_internal_stage;

---------------------Data into external stage------------------------


-- Define file format, storage integration, and create an external stage.
-- Define file format for the CSV file.
CREATE OR REPLACE FILE FORMAT assignment_db.my_schema.my_csv_format
    TYPE = CSV
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
    NULL_IF = ('NULL', 'null')
    EMPTY_FIELD_AS_NULL = TRUE;

-- Create a storage integration for external stage (assuming AWS S3).
CREATE OR REPLACE STORAGE INTEGRATION s3_int2
    TYPE = EXTERNAL_STAGE
    STORAGE_PROVIDER = S3
    ENABLED = TRUE
    STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::339713021228:role/myrole'
    STORAGE_ALLOWED_LOCATIONS = ('s3://myawsbucketpoornesh/');

-- Create an external stage using the storage integration.
CREATE OR REPLACE STAGE my_s3_stage
    STORAGE_INTEGRATION = s3_int2
    URL = 's3://myawsbucketpoornesh/people-10000.csv'
    FILE_FORMAT = my_csv_format;

-- Describe the details of the storage integration.
DESC INTEGRATION s3_int2;


--------------------Q7 to just create variant version of our data---------------------- 


-- Create the table with audit columns.
CREATE OR REPLACE TABLE my_schema.people_variant (
    index INT,
    user_id VARCHAR(300),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    sex VARCHAR(10),
    email VARCHAR(100),
    phone VARCHAR(30),
    date_of_birth DATE,
    job_title VARCHAR(100),
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR(30) DEFAULT 'Local',
    file_name VARCHAR(30) DEFAULT 'people-10000.csv'
);


-- Copy data from the internal stage to the 'people_variant' table.
COPY INTO people_variant(index, user_id, first_name, last_name, sex, email, phone, date_of_birth, job_title)
FROM @your_internal_stage/people-10000.csv.gz
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE'
PURGE = TRUE;





/************************************************************************************************************************************/

-- Question 9: Load data into the tables using copy into statements. In one table load from the internal stage and in another from the external ( 10 ).

---------------------Data from internal stage------------------------


-- Create the table with audit columns.
CREATE OR REPLACE TABLE my_schema.people (
    index INT,
    user_id VARCHAR(300),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    sex VARCHAR(10),
    email VARCHAR(100),
    phone VARCHAR(30),
    date_of_birth DATE,
    job_title VARCHAR(100),
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR(30) DEFAULT 'Local',
    file_name VARCHAR(30) DEFAULT 'people-10000.csv'
);


-- Copy data from the internal stage to the 'people' table.
COPY INTO people(index, user_id, first_name, last_name, sex, email, phone, date_of_birth, job_title)
FROM @your_internal_stage/people-10000.csv.gz
FILE_FORMAT = (TYPE = 'CSV' SKIP_HEADER = 1)
ON_ERROR = 'CONTINUE'
PURGE = TRUE;


SELECT * FROM people limit 5;

---------------------Data from external stage------------------------


-- Create an identical table for external data.
CREATE OR REPLACE TABLE my_schema.people_ext (
    index INT,
    user_id VARCHAR(300),
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    sex VARCHAR(10),
    email VARCHAR(100),
    phone VARCHAR(30),
    date_of_birth DATE,
    job_title VARCHAR(100),
    elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    elt_by VARCHAR(30) DEFAULT 'Local',
    file_name VARCHAR(30) DEFAULT 'people-10000.csv'
);


-- Copy data from the external stage to the 'people_ext' table.
COPY INTO my_schema.people_ext (index, user_id, first_name, last_name, sex, email, phone, date_of_birth, job_title)
FROM @my_s3_stage;

/************************************************************************************************************************************/
-- Question 10: Upload any parquet file to the stage location and infer the schema of the file ( 5 ).

-- Use the ADMIN role for these tasks.
USE ROLE ADMIN;

-- Specify the path to the Parquet file on your local machine.
-- Replace '/Users/poornesh/Downloads/userdata1.parquet' with the actual path of yours.
PUT file:///Users/poornesh/Downloads/userdata1.parquet @YOUR_INTERNAL_STAGE;

/************************************************************************************************************************************/
-- Question 11: Run a SELECT query on the staged Parquet file without loading it into a Snowflake table ( 5 ).

-- Create file format for Parquet files.
CREATE FILE FORMAT my_parquet_format TYPE = PARQUET;

-- Run a SELECT query on the staged Parquet file without loading it into a Snowflake table.
SELECT *
FROM TABLE(INFER_SCHEMA(LOCATION => '@YOUR_INTERNAL_STAGE', FILE_FORMAT => 'my_parquet_format'))
LIMIT 5;


/************************************************************************************************************************************/

-- Question 12: Add masking policy to the PII columns such that fields like email, phone number, etc. show as **masked** to a user with the developer role. If the role is PII the value of these columns should be visible ( 15 ).

-- Create or replace masking policy for PII columns.
CREATE OR REPLACE MASKING POLICY pii_masking_policy AS (val STRING) RETURNS STRING ->
  CASE
    WHEN CURRENT_ROLE() = 'DEVELOPER' THEN '**********'
    ELSE val
  END;

-- Apply masking policies to PII columns in the 'people' table.
ALTER TABLE my_schema.people MODIFY COLUMN email SET MASKING POLICY pii_masking_policy;
ALTER TABLE my_schema.people MODIFY COLUMN phone SET MASKING POLICY pii_masking_policy;

-- Apply masking policies to PII columns in the 'people_ext' table.
ALTER TABLE my_schema.people_ext MODIFY COLUMN email SET MASKING POLICY pii_masking_policy;
ALTER TABLE my_schema.people_ext MODIFY COLUMN phone SET MASKING POLICY pii_masking_policy;


-- Switch role to developer.
USE ROLE developer;

-- Select query to demonstrate masking behavior.
SELECT * FROM my_schema.people LIMIT 5;

/************************************************************************************************************************************/



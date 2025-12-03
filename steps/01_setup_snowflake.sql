-- ----------------------------------------------------------------------------
-- Step #1: Accept Anaconda Terms & Conditions
-- ----------------------------------------------------------------------------
-- See Getting Started section in Third-Party Packages (https://docs.snowflake.com/en/developer-guide/udf/python/udf-python-packages.html#getting-started)


-- ----------------------------------------------------------------------------
-- Step #2: Create the account level objects
-- ----------------------------------------------------------------------------
USE ROLE ACCOUNTADMIN;

-- Roles
SET MY_USER = CURRENT_USER();

-- ----------------------------------------------------------------------------
-- Step #3: Create the database level objects
-- ----------------------------------------------------------------------------
USE WAREHOUSE LOADING_XS_WH;

-- Create file format types for ease of loading data
CREATE FILE FORMAT IF NOT EXISTS CSV_FORMAT
    TYPE = CSV
    FIELD_DELIMITER = ','
    SKIP_HEADER = 1
;
CREATE FILE FORMAT IF NOT EXISTS XML_FORMAT
    TYPE = XML
    COMPRESSION = NONE
;
-- ----------------------------------------------------------------------------
-- BRONZE (stage 1) of data lake; raw data

-- CREATE DATABASE IF NOT EXISTS ALL_RAW_DB;
-- GRANT OWNERSHIP ON DATABASE ALL_RAW_DB TO ROLE SYSADMIN;
-- CREATE SCHEMA IF NOT EXISTS VISITS;
USE DATABASE ALL_RAW_DB;
USE SCHEMA VISITS;
CREATE OR REPLACE TABLE ALL_RAW_DB.VISITS.RAW_VISITS_FRAHMAN_SNOWPARK (
	SHOPPERTRAK_SITE_ID VARCHAR(50) NOT NULL,
	ORBIT SMALLINT,
	INCREMENT_START TIMESTAMP_NTZ NOT NULL,
	ENTERS INT,
	EXITS INT,
	IS_HEALTHY_DATA BOOLEAN NOT NULL,
	SOURCE_FILE_NAME VARCHAR(250) NOT NULL,
	SOURCE_FILE_ROW_NO NUMBER NOT NULL,
	SOURCE_LOAD_TIMESTAMP TIMESTAMP_LTZ NOT NULL
);
CREATE or REPLACE TABLE ALL_RAW_DB.VISITS.UNSTRUCTURED_VISITS_FRAHMAN_SNOWPARK (
	SRC VARIANT NOT NULL
);
-- Create staging for ShopperTrak visits CSV data
CREATE STAGE IF NOT EXISTS shoppertrak_visits_snowpark
    COMMENT = 'Stage for raw ShopperTrak visits CSV data (filled via Snowpark)'
    STORAGE_INTEGRATION = visits_s3_integration
    URL = 's3://nypl-data-lake-test/shoppertrak_visits_csv/'
    FILE_FORMAT = (TYPE = CSV);
-- Validate stage files exists
LIST @shoppertrak_visits_snowpark;
-- ----------------------------------------------------------------------------
-- SILVER (stage 2) of data lake; transformed data

-- CREATE DATABASE IF NOT EXISTS ALL_INT_DB;
-- GRANT OWNERSHIP ON DATABASE ALL_INT_DB TO ROLE SYSADMIN;
-- CREATE SCHEMA IF NOT EXISTS VISITS;
USE DATABASE ALL_INT_DB;
USE SCHEMA VISITS;
CREATE or REPLACE DYNAMIC TABLE ALL_INT_DB.VISITS.HEALTHY_VISITS_FRAHMAN_SNOWPARK(
	SHOPPERTRAK_SITE_ID,
	ORBIT,
	INCREMENT_START,
	ENTERS,
	EXITS,
	SOURCE_FILE_NAME,
	SOURCE_FILE_ROW_NO,
	LOAD_TIMESTAMP
) target_lag = '5 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = LOADING_XS_WH
 as
SELECT
    shoppertrak_site_id,
    orbit,
    increment_start,
    enters,
    exits,
    source_file_name,
    source_file_row_no,
    current_timestamp() AS load_timestamp
FROM all_raw_db.visits.raw_visits_frahman
WHERE is_healthy_data = TRUE;

CREATE or REPLACE DYNAMIC TABLE ALL_INT_DB.VISITS.UNHEALTHY_VISITS_FRAHMAN_SNOWPARK(
	SHOPPERTRAK_SITE_ID,
	ORBIT,
	INCREMENT_START,
	ENTERS,
	EXITS,
	SOURCE_FILE_NAME,
	SOURCE_FILE_ROW_NO,
	LOAD_TIMESTAMP
) target_lag = '5 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = LOADING_XS_WH
 as
SELECT
    shoppertrak_site_id,
    orbit,
    increment_start,
    enters,
    exits,
    source_file_name,
    source_file_row_no,
    current_timestamp() AS load_timestamp
FROM all_raw_db.visits.raw_visits_frahman
WHERE is_healthy_data = FALSE;
-- ----------------------------------------------------------------------------
-- GOLD (stage 3) of data lake; business-ready data

-- CREATE DATABASE IF NOT EXISTS ALL_PRS_DB;
-- GRANT OWNERSHIP ON DATABASE ALL_PRS_DB TO ROLE SYSADMIN;
-- CREATE SCHEMA IF NOT EXISTS VISITS;
-- Warehouses
-- CREATE WAREHOUSE IF NOT EXISTS LOADING_XS_WH WAREHOUSE_SIZE = XSMALL, AUTO_SUSPEND = 300, AUTO_RESUME= TRUE;
-- GRANT OWNERSHIP ON WAREHOUSE LOADING_XS_WH TO ROLE SYSADMIN;
USE DATABASE ALL_PRS_DB;
USE SCHEMA VISITS;

create or replace dynamic table ALL_PRS_DB.VISITS.DAILY_VISITS_ORBIT_FRAHMAN_SNOWPARK(
	SHOPPERTRAK_SITE_ID,
	ORBIT,
	VISITS_DATE,
	AGG_ENTERS,
	AGG_EXITS,
	LOAD_TIMESTAMP
) target_lag = '5 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = LOADING_XS_WH
 as
SELECT 
shoppertrak_site_id, visits_date, agg_enters, agg_exits, current_timestamp() AS load_timestamp
FROM (
    SELECT
        shoppertrak_site_id,
        TO_DATE(increment_start) AS visits_date,
        SUM(enters) AS agg_enters,
        SUM(exits) AS agg_exits
    FROM all_int_db.visits.healthy_visits_frahman
    GROUP BY shoppertrak_site_id, visits_date
);

CREATE or REPLACE DYNAMIC TABLE ALL_PRS_DB.VISITS.DAILY_VISITS_ORBIT_FRAHMAN_SNOWPARK(
	SHOPPERTRAK_SITE_ID,
	ORBIT,
	VISITS_DATE,
	AGG_ENTERS,
	AGG_EXITS,
	LOAD_TIMESTAMP
) target_lag = '5 minutes' refresh_mode = AUTO initialize = ON_CREATE warehouse = LOADING_XS_WH
 as
SELECT 
shoppertrak_site_id, orbit, visits_date, agg_enters, agg_exits, current_timestamp() AS load_timestamp
FROM (
    SELECT
        shoppertrak_site_id,
        orbit,
        TO_DATE(increment_start) AS visits_date,
        SUM(enters) AS agg_enters,
        SUM(exits) AS agg_exits
    FROM all_int_db.visits.healthy_visits_frahman
    GROUP BY shoppertrak_site_id, orbit, visits_date
);
-- ----------------------------------------------------------------------------
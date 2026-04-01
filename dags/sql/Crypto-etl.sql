

/* ============================================================
   STEP 0 — Context (Role / Warehouse / DB / Schema)
   ============================================================ */
USE ROLE ACCOUNTADMIN;

CREATE OR REPLACE WAREHOUSE ETL_WH
  WAREHOUSE_SIZE = XSMALL
  AUTO_SUSPEND   = 60
  AUTO_RESUME    = TRUE;

CREATE OR REPLACE DATABASE CRYPTO_DB;
CREATE OR REPLACE SCHEMA CRYPTO_DB.RAW;

USE WAREHOUSE ETL_WH;
USE DATABASE CRYPTO_DB;
USE SCHEMA RAW;


/* ============================================================
   STEP 1 — Storage Integration (Snowflake <-> AWS IAM Role)
   NOTE: This assumes your IAM Role trust policy is already set
         using the values from:
         DESC INTEGRATION s3_crypto_int;
   ============================================================ 
CREATE OR REPLACE STORAGE INTEGRATION s3_crypto_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::911337539817:role/snowflake_s3_crypto_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://test-for-etl-proj/raw/');

-- Verify Snowflake generated IAM user ARN + external ID
DESC INTEGRATION s3_crypto_int;
*/

/* ============================================================
   STEP 2 — File Format + External Stage (points to S3)
   IMPORTANT: Keep trailing slash in URL to match allowed location
   ============================================================ */
CREATE OR REPLACE FILE FORMAT crypto_json_ff
  TYPE = JSON;

CREATE OR REPLACE STAGE s3_crypto_stage
  STORAGE_INTEGRATION = s3_crypto_int
  URL = 's3://test-for-etl-proj/raw/'
  FILE_FORMAT = crypto_json_ff;

--SHOW STAGES IN SCHEMA CRYPTO_DB.RAW;

-- Verify Snowflake can see the files
LIST @s3_crypto_stage/api/;


/* ============================================================
   STEP 3 — Raw Landing Table (keeps original JSON)
   ============================================================ */
CREATE OR REPLACE TABLE crypto_prices_raw (
  file_name STRING,
  load_ts   TIMESTAMP_NTZ,
  payload   VARIANT
);


/* ============================================================
   STEP 4 — Load from S3 into Raw Table (COPY INTO)
   - FORCE=TRUE re-loads even if Snowflake thinks it was loaded
   - PATTERN ensures only crypto_prices_*.json are loaded
   ============================================================ */
COPY INTO crypto_prices_raw (file_name, load_ts, payload)
FROM (
  SELECT
    METADATA$FILENAME,
    CURRENT_TIMESTAMP(),
    $1
  FROM @s3_crypto_stage/api/
)
PATTERN = '.*crypto_prices_.*\.json'
FORCE = TRUE;

-- Quick sanity check (latest loaded rows)
SELECT
  file_name,
  load_ts,
  payload:extracted_at::timestamp_ntz AS extracted_at,
  payload:data:bitcoin:usd::number    AS btc_usd,
  payload:data:ethereum:usd::number   AS eth_usd
FROM crypto_prices_raw
ORDER BY load_ts DESC
LIMIT 10;


/* ============================================================
   STEP 5 — Deduplicate Raw (keep latest load per file_name)
   ============================================================ */
DELETE FROM crypto_prices_raw
WHERE (file_name, load_ts) IN (
  SELECT file_name, load_ts
  FROM (
    SELECT
      file_name,
      load_ts,
      ROW_NUMBER() OVER (PARTITION BY file_name ORDER BY load_ts DESC) AS rn
    FROM crypto_prices_raw
  )
  WHERE rn > 1
);

-- Confirm no duplicates remain
SELECT file_name, COUNT(*) AS cnt
FROM crypto_prices_raw
GROUP BY file_name
ORDER BY cnt DESC;


/* ============================================================
   STEP 6 — Analytics Table (flatten JSON into columns)
   (This makes Tableau work much more smoothly.)
   ============================================================ */
CREATE OR REPLACE TABLE crypto_prices_analytics AS
SELECT
  payload:extracted_at::timestamp_ntz AS extracted_at,
  payload:data:bitcoin:usd::number    AS btc_usd,
  payload:data:ethereum:usd::number   AS eth_usd,
  file_name,
  load_ts
FROM crypto_prices_raw;

SELECT *
FROM crypto_prices_analytics
ORDER BY extracted_at DESC
LIMIT 10;


/* ============================================================
   STEP 7 — Tableau-friendly VIEW (latest per extracted_at)
   - Prevents duplicates if same extracted_at loads again
   - Tableau should connect to this view
   ============================================================ */
CREATE OR REPLACE VIEW vw_crypto_prices_latest AS
SELECT
  extracted_at,
  btc_usd,
  eth_usd
FROM crypto_prices_analytics
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY extracted_at
  ORDER BY load_ts DESC
) = 1;

SELECT * FROM vw_crypto_prices_latest
ORDER BY extracted_at DESC
LIMIT 20;


/* ============================================================
   OPTIONAL — Environment checks
   ============================================================ */
SELECT CURRENT_ACCOUNT(), CURRENT_REGION();

--DESC INTEGRATION s3_crypto_int;


CREATE OR REPLACE VIEW CRYPTO_DB.RAW.VW_CRYPTO_PRICES_HISTORY AS
SELECT
  extracted_at,
  btc_usd,
  eth_usd
FROM CRYPTO_DB.RAW.CRYPTO_PRICES_ANALYTICS
ORDER BY extracted_at;

-- =====================================================Delete ===========================================

/*
USE ROLE ACCOUNTADMIN;
USE DATABASE CRYPTO_DB;
USE SCHEMA RAW;

DROP VIEW IF EXISTS VW_CRYPTO_PRICES_LATEST;
DROP VIEW IF EXISTS VW_CRYPTO_PRICES_HISTORY;

DROP TABLE IF EXISTS CRYPTO_PRICES_ANALYTICS;
DROP TABLE IF EXISTS CRYPTO_PRICES_RAW;

DROP STAGE IF EXISTS S3_CRYPTO_STAGE;

DROP FILE FORMAT IF EXISTS CRYPTO_JSON_FF;
DROP FILE FORMAT IF EXISTS FF_JSON;



USE ROLE ACCOUNTADMIN;
DROP STORAGE INTEGRATION IF EXISTS S3_CRYPTO_INT;


USE ROLE ACCOUNTADMIN;

DROP SCHEMA IF EXISTS CRYPTO_DB.RAW;
DROP DATABASE IF EXISTS CRYPTO_DB;

DROP WAREHOUSE IF EXISTS ETL_WH;
*/
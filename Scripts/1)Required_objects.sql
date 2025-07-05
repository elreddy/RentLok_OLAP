--Database and Schema Creation:

CREATE OR REPLACE DATABASE rentlok_prd;
CREATE OR REPLACE SCHEMA rentlok_prd.bronze;
CREATE OR REPLACE SCHEMA rentlok_prd.silver;
CREATE OR REPLACE SCHEMA rentlok_prd.gold;

--File Format Creation:
---------------------
CREATE OR REPLACE FILE FORMAT rentlok_prd.bronze.my_csv_format
  TYPE = 'CSV'
  FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  SKIP_HEADER = 1
  FIELD_DELIMITER = ',';

--S3 Integration:
--------------------
CREATE OR REPLACE STORAGE INTEGRATION S3_role_integration
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = "<your_arn>/snowflake_s3"
  STORAGE_ALLOWED_LOCATIONS = ("s3://rentlok/");

--Stage Creation:
--------------------
CREATE OR REPLACE STAGE rentlok_prd.bronze.s3_rentlok
url = ('s3://rentlok/')
file_format = my_csv_format
storage_integration = S3_role_integration;
-- Bronze layer stream to stage load tasks.
-- Execute these in Silver schema as we cant have dependency between tasks in different schemas.
--===============================================================================================

--Root task to initiate the Pipeline:
create or replace task RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
	warehouse=COMPUTE_WH
	schedule='USING CRON 0 21 * * * UTC'
	COMMENT='Master orchestrator for Rentlok Pipeline'
	as SELECT 1;


-- PROPERTIES:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_properties
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table properties'
  QUERY_TAG = 'load_brz_streamTostage_properties'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_PROPERTIES;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_PROPERTIES(
        PROPERTY_ID,
        PROPERTY_NAME,
        ADDRESS,
        NO_OF_ROOMS,
        OWNER_ID,
        CREATED_AT,
        IS_ACTIVE
    )
    SELECT 
        PROPERTY_ID,
        PROPERTY_NAME,
        ADDRESS,
        NO_OF_ROOMS,
        OWNER_ID,
        CREATED_AT,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_properties
    WHERE METADATA$ACTION IN ('INSERT','UPDATE');
END;


--ROOMS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_rooms
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table rooms'
  QUERY_TAG = 'load_brz_streamTostage_rooms'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_ROOMS;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_ROOMS (
        ROOM_ID,
        ROOM_NO,
        FLOOR_NO,
        PROPERTY_ID,
        OPERATIONAL_STATUS,
        ROOM_TYPE,
        RENT_PER_MONTH,
        IS_ACTIVE
    )
    SELECT
        ROOM_ID,
        ROOM_NO,
        FLOOR_NO,
        PROPERTY_ID,
        OPERATIONAL_STATUS,
        ROOM_TYPE,
        RENT_PER_MONTH,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_rooms
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE');
END;


--REQUESTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_requests
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table requests'
  QUERY_TAG = 'load_brz_streamTostage_requests'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_REQUESTS;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_REQUESTS (
        REQUEST_ID,
        PROPERTY_ID,
        TENANT_NAME,
        PHONE_NO,
        DETAILS,
        REQUEST_DATE,
        IS_ACTIVE
    )
    SELECT
        REQUEST_ID,
        PROPERTY_ID,
        TENANT_NAME,
        PHONE_NO,
        DETAILS,
        REQUEST_DATE,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_requests
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE');
END;


--TENANTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_tenants
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table tenants'
  QUERY_TAG = 'load_brz_streamTostage_tenants'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_TENANTS;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_TENANTS (
        TENANT_ID,
        NAME,
        PHONE_NO,
        DETAILS,
        IS_ACTIVE
    )
    SELECT
        TENANT_ID,
        NAME,
        PHONE_NO,
        DETAILS,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_tenants
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE');
END;


--BOOKINGS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_bookings
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table bookings'
  QUERY_TAG = 'load_brz_streamTostage_bookings'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_BOOKINGS;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_BOOKINGS (
        BOOKING_ID,
        ROOM_ID,
        TENANT_ID,
        PROPERTY_ID,
        MOVE_IN_DATE,
        MOVE_OUT_DATE,
        STATUS,
        IS_ACTIVE
    )
    SELECT
        BOOKING_ID,
        ROOM_ID,
        TENANT_ID,
        PROPERTY_ID,
        MOVE_IN_DATE,
        MOVE_OUT_DATE,
        STATUS,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_bookings
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE');
END;


--PAYMENTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_bronze_stream_to_stage_load_payments
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table payments'
  QUERY_TAG = 'load_brz_streamTostage_payments'
  AFTER RENTLOK_PRD.SILVER.TASK_RENTLOK_PIPELINE_STARTER
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.BRONZE.STAGE_PAYMENTS;
    INSERT INTO RENTLOK_PRD.BRONZE.STAGE_PAYMENTS (
        PAYMENT_ID,
        BOOKING_ID,
        PAYMENT_TYPE,
        PAYMENT_STATUS,
        AMOUNT,
        PAYMENT_DATE,
        PAYMENT_MONTH,
        IS_ACTIVE
    )
    SELECT
        PAYMENT_ID,
        BOOKING_ID,
        PAYMENT_TYPE,
        PAYMENT_STATUS,
        AMOUNT,
        PAYMENT_DATE,
        PAYMENT_MONTH,
        IS_ACTIVE
    FROM RENTLOK_PRD.BRONZE.stream_payments
    WHERE METADATA$ACTION IN ('INSERT', 'UPDATE');
END;
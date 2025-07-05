-- Silver layer tasks for bronze stage table to silver table load and silver stream to stage table load.
-- This script contains tasks to load data from bronze stage tables into silver tables for properties, rooms, requests, tenants, bookings, and payments.

--PROPERTIES:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_properties
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table properties'
  QUERY_TAG = 'load_brz_stage_to_sil_table_properties'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_PROPERTIES
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.properties AS tgt
    USING (
      SELECT
        sr.property_id,
        COALESCE(NULLIF(sr.property_name, ''), 'UNKNOWN') AS property_name,
        COALESCE(NULLIF(sr.address, ''), 'UNKNOWN') AS address,
        COALESCE(CAST(sr.no_of_rooms AS NUMBER), 0) AS no_of_rooms,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI') AS batch_id,
        CURRENT_TIMESTAMP() AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_properties sr
    ) AS src
      ON tgt.property_id = src.property_id
    
    WHEN MATCHED THEN
      UPDATE SET
        property_name = src.property_name,
        address       = src.address,
        no_of_rooms   = src.no_of_rooms,
        is_active     = src.is_active,
        batch_id      = src.batch_id,
        updated_at    = src.now_ts
    
    WHEN NOT MATCHED THEN
      INSERT (
        property_id,
        property_name,
        address,
        no_of_rooms,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.property_id,
        src.property_name,
        src.address,
        src.no_of_rooms,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;

CREATE OR REPLACE TASK rentlok_prd.silver.task_stream_to_stage_load_properties
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table properties'
  QUERY_TAG = 'load_streamTostage_table_properties'
  AFTER rentlok_prd.silver.task_stage_to_table_load_properties
AS
BEGIN
TRUNCATE TABLE RENTLOK_PRD.SILVER.stage_properties;
INSERT INTO RENTLOK_PRD.SILVER.stage_properties(
    sk_property,
    property_id,  
    property_name,
    address,      
    no_of_rooms,  
    is_active,    
    batch_id,    
    inserted_at,  
    updated_at    
)
SELECT 
    sk_property,
    property_id,  
    property_name,
    address,      
    no_of_rooms,  
    is_active,    
    batch_id,    
    inserted_at,  
    updated_at   
FROM RENTLOK_PRD.SILVER.stream_properties
WHERE METADATA$ACTION IN ('INSERT','UPDATE');
END;



--ROOMS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_rooms
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table rooms'
  QUERY_TAG = 'load_brz_stage_to_sil_table_rooms'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_ROOMS
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.rooms AS tgt
    USING (
      SELECT
        sr.room_id,
        sr.property_id,
        COALESCE(NULLIF(sr.room_no, ''), 'UNKNOWN')         AS room_no,
        COALESCE(CAST(sr.floor_no AS NUMBER), 0)            AS floor_no,
        COALESCE(NULLIF(sr.operational_status, ''), 'UNKNOWN') AS operational_status,
        COALESCE(NULLIF(sr.room_type, ''), 'UNKNOWN')          AS room_type,
        COALESCE(CAST(sr.rent_per_month AS FLOAT), 0) AS rent_per_month,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI')      AS batch_id,
        CURRENT_TIMESTAMP()                                  AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_rooms AS sr
    ) AS src
      ON tgt.room_id = src.room_id
    WHEN MATCHED THEN
      UPDATE SET
        property_id        = src.property_id,
        room_no            = src.room_no,
        floor_no           = src.floor_no,
        operational_status = src.operational_status,
        room_type          = src.room_type,
        rent_per_month     = src.rent_per_month,
        is_active          = src.is_active,
        batch_id           = src.batch_id,
        updated_at         = src.now_ts
    
    WHEN NOT MATCHED THEN
      INSERT (
        room_id,
        property_id,
        room_no,
        floor_no,
        operational_status,
        room_type,
        rent_per_month,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.room_id,
        src.property_id,
        src.room_no,
        src.floor_no,
        src.operational_status,
        src.room_type,
        src.rent_per_month,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;

CREATE OR REPLACE TASK rentlok_prd.silver.task_stream_to_stage_load_rooms
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table rooms'
  QUERY_TAG = 'load_streamTostage_table_rooms'
  AFTER rentlok_prd.silver.task_stage_to_table_load_rooms
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.SILVER.stage_rooms;
    INSERT INTO RENTLOK_PRD.SILVER.stage_rooms(
        sk_room,           
        room_id,           
        property_id ,      
        room_no,           
        floor_no,          
        operational_status,
        room_type,         
        rent_per_month,    
        is_active,         
        batch_id,          
        inserted_at,       
        updated_at 
    )
    SELECT 
        sk_room,           
        room_id,           
        property_id ,      
        room_no,           
        floor_no,          
        operational_status,
        room_type,         
        rent_per_month,    
        is_active,         
        batch_id,          
        inserted_at,       
        updated_at 
    FROM RENTLOK_PRD.SILVER.stream_rooms
    WHERE METADATA$ACTION IN ('INSERT','UPDATE');
END;

--REQUESTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_requests
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table requests'
  QUERY_TAG = 'load_brz_stage_to_sil_table_requests'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_REQUESTS
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.requests AS tgt
    USING (
      SELECT
        sr.request_id,
        sr.property_id,
        COALESCE(NULLIF(sr.tenant_name, ''), 'UNKNOWN')    AS tenant_name,
        COALESCE(NULLIF(sr.phone_no,      ''), 'UNKNOWN')  AS phone_no,
        COALESCE(NULLIF(sr.details,       ''), 'NONE')     AS details,
        CAST(sr.request_date AS DATE)                     AS request_date,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI')      AS batch_id,
        CURRENT_TIMESTAMP()                                  AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_requests AS sr
    ) AS src
      ON tgt.request_id = src.request_id
    WHEN MATCHED THEN
      UPDATE SET
        property_id  = src.property_id,
        tenant_name  = src.tenant_name,
        phone_no     = src.phone_no,
        details      = src.details,
        request_date = src.request_date,
        is_active    = src.is_active,
        batch_id     = src.batch_id,
        updated_at   = src.now_ts
    WHEN NOT MATCHED THEN
      INSERT (
        request_id,
        property_id,
        tenant_name,
        phone_no,
        details,
        request_date,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.request_id,
        src.property_id,
        src.tenant_name,
        src.phone_no,
        src.details,
        src.request_date,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;

CREATE OR REPLACE TASK rentlok_prd.silver.task_stream_to_stage_load_requests
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table requests'
  QUERY_TAG = 'load_streamTostage_table_requests'
  AFTER rentlok_prd.silver.task_stage_to_table_load_requests
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.SILVER.stage_requests;
    INSERT INTO RENTLOK_PRD.SILVER.stage_requests(
        sk_request,  
        request_id,  
        property_id, 
        tenant_name ,
        phone_no,    
        details,     
        request_date,
        is_active,   
        batch_id,    
        inserted_at, 
        updated_at  
    )
    SELECT 
        sk_request,  
        request_id,  
        property_id, 
        tenant_name ,
        phone_no,    
        details,     
        request_date,
        is_active,   
        batch_id,    
        inserted_at, 
        updated_at
    FROM RENTLOK_PRD.SILVER.stream_requests
    WHERE METADATA$ACTION IN ('INSERT','UPDATE');
END;


--TENANTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_tenants
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table tenants'
  QUERY_TAG = 'load_brz_stage_to_sil_table_renants'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_TENANTS
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.tenants AS tgt
    USING (
      SELECT
        sr.tenant_id,
        COALESCE(NULLIF(sr.name, ''), 'UNKNOWN')    AS tenant_name,
        COALESCE(NULLIF(sr.phone_no, ''), 'UNKNOWN')AS phone_no,
        COALESCE(NULLIF(sr.details, ''), 'NONE')    AS details,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI') AS batch_id,
        CURRENT_TIMESTAMP()                             AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_tenants AS sr
    ) AS src
      ON tgt.tenant_id = src.tenant_id
    WHEN MATCHED THEN
      UPDATE SET
        tenant_name = src.tenant_name,
        phone_no    = src.phone_no,
        details     = src.details,
        is_active   = src.is_active,
        batch_id    = src.batch_id,
        updated_at  = src.now_ts
    WHEN NOT MATCHED THEN
      INSERT (
        tenant_id,
        tenant_name,
        phone_no,
        details,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.tenant_id,
        src.tenant_name,
        src.phone_no,
        src.details,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;

CREATE OR REPLACE TASK rentlok_prd.silver.task_stream_to_stage_load_tenants
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load stream data into stage table tenants'
  QUERY_TAG = 'load_streamTostage_table_tenants'
  AFTER rentlok_prd.silver.task_stage_to_table_load_tenants
AS
BEGIN
    TRUNCATE TABLE RENTLOK_PRD.SILVER.stage_tenants;
    INSERT INTO RENTLOK_PRD.SILVER.stage_tenants(
        sk_tenant,   
        tenant_id,   
        tenant_name, 
        phone_no,    
        details,     
        is_active,   
        batch_id,    
        inserted_at, 
        updated_at  
    )
    SELECT 
        sk_tenant,  
        tenant_id,  
        tenant_name,
        phone_no,   
        details,    
        is_active,  
        batch_id,   
        inserted_at,
        updated_at  
    FROM RENTLOK_PRD.SILVER.stream_tenants
    WHERE METADATA$ACTION IN ('INSERT','UPDATE');
END;


--BOOKINGS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_bookings
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table bookings'
  QUERY_TAG = 'load_brz_stage_to_sil_table_bookings'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_BOOKINGS
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.bookings AS tgt
    USING (
      SELECT
        sr.booking_id,
        sr.room_id,
        sr.tenant_id,
        sr.property_id,
        CAST(sr.move_in_date AS DATE)                         AS move_in_date,
        CAST(sr.move_out_date AS DATE)                        AS move_out_date,
        COALESCE(NULLIF(sr.status, ''), 'UNKNOWN')             AS status,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI')         AS batch_id,
        CURRENT_TIMESTAMP()                                     AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_bookings AS sr
    ) AS src
      ON tgt.booking_id = src.booking_id
    WHEN MATCHED THEN
      UPDATE SET
        room_id       = src.room_id,
        tenant_id     = src.tenant_id,
        property_id   = src.property_id,
        move_in_date  = src.move_in_date,
        move_out_date = src.move_out_date,
        status        = src.status,
        is_active     = src.is_active,
        batch_id      = src.batch_id,
        updated_at    = src.now_ts
    WHEN NOT MATCHED THEN
      INSERT (
        booking_id,
        room_id,
        tenant_id,
        property_id,
        move_in_date,
        move_out_date,
        status,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.booking_id,
        src.room_id,
        src.tenant_id,
        src.property_id,
        src.move_in_date,
        src.move_out_date,
        src.status,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;


--PAYMNETS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_table_load_payments
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load bronze stage data into silver table payments'
  QUERY_TAG = 'load_brz_stage_to_sil_table_payments'
  AFTER RENTLOK_PRD.SILVER.TASK_BRONZE_STREAM_TO_STAGE_LOAD_PAYMENTS
AS
BEGIN
    MERGE INTO RENTLOK_PRD.SILVER.payments AS tgt
    USING (
      SELECT
        sr.payment_id,
        sr.booking_id,
        COALESCE(NULLIF(sr.payment_type, ''),   'UNKNOWN')      AS payment_type,
        COALESCE(NULLIF(sr.payment_status, ''), 'UNKNOWN')      AS payment_status,
        COALESCE(CAST(sr.amount AS FLOAT), 0)                  AS amount,
        CAST(sr.payment_date AS DATE)                            AS payment_date,
        COALESCE(NULLIF(sr.payment_month, ''),   TO_CHAR(sr.payment_date, 'YYYY-MM')) AS payment_month,
        sr.is_active AS is_active,
        TO_CHAR(CURRENT_TIMESTAMP(), 'YYYYMMDD_HH24MI')           AS batch_id,
        CURRENT_TIMESTAMP()                                       AS now_ts
      FROM RENTLOK_PRD.BRONZE.stage_payments AS sr
    ) AS src
      ON tgt.payment_id = src.payment_id
    WHEN MATCHED THEN
      UPDATE SET
        booking_id     = src.booking_id,
        payment_type   = src.payment_type,
        payment_status = src.payment_status,
        amount         = src.amount,
        payment_date   = src.payment_date,
        payment_month  = src.payment_month,
        is_active      = src.is_active,
        batch_id       = src.batch_id,
        updated_at     = src.now_ts
    WHEN NOT MATCHED THEN
      INSERT (
        payment_id,
        booking_id,
        payment_type,
        payment_status,
        amount,
        payment_date,
        payment_month,
        is_active,
        batch_id,
        inserted_at,
        updated_at
      )
      VALUES (
        src.payment_id,
        src.booking_id,
        src.payment_type,
        src.payment_status,
        src.amount,
        src.payment_date,
        src.payment_month,
        src.is_active,
        src.batch_id,
        src.now_ts,
        src.now_ts
      );
END;

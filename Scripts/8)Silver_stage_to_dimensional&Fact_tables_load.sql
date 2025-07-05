-- Silver Stage to Dimensional Tables Load Tasks and dimensional tables to fact tables load tasks.

--PROPERTIES:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_dimension_table_load_properties
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load silver stage data into gold dimensional table properties'
  QUERY_TAG = 'load_sil_stage_to_gold_table_properties'
  AFTER rentlok_prd.silver.task_stream_to_stage_load_properties
AS
BEGIN
    MERGE INTO rentlok_prd.gold.dim_properties AS tgt
    USING (
      SELECT
        sr.property_id,
        sr.property_name,
        sr.address            AS property_address,
        sr.no_of_rooms,
        sr.is_active          AS is_active,
        sr.updated_at         AS src_updated_at
      FROM RENTLOK_PRD.SILVER.stage_properties sr
    ) AS src
      ON tgt.property_id = src.property_id
         AND tgt.is_current = TRUE
    WHEN MATCHED
      AND (
           src.property_name    <> tgt.property_name
        OR src.property_address <> tgt.property_address
        OR src.no_of_rooms      <> tgt.no_of_rooms
        OR src.is_active        <> tgt.is_active
      )
    THEN
      UPDATE SET
        end_date    = CURRENT_DATE() - 1,
        is_current  = FALSE,
        updated_at  = CURRENT_TIMESTAMP();
    
    MERGE INTO rentlok_prd.gold.dim_properties AS tgt
    USING (
      SELECT
        sr.property_id,
        sr.property_name,
        sr.address            AS property_address,
        sr.no_of_rooms,
        sr.is_active          AS is_active,
        sr.updated_at         AS src_updated_at
      FROM RENTLOK_PRD.SILVER.stage_properties sr
    ) AS src
      ON tgt.property_id = src.property_id
         AND tgt.is_current = TRUE
    WHEN NOT MATCHED
    THEN
      INSERT (
        property_id,
        property_name,
        property_address,
        no_of_rooms,
        is_active,
        start_date,
        end_date,
        is_current,
        inserted_at,
        updated_at
      )
      VALUES (
        src.property_id,
        src.property_name,
        src.property_address,
        src.no_of_rooms,
        src.is_active,
        CURRENT_DATE(),        
        NULL,                  
        TRUE,                  
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;


--ROOMS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_dimension_table_load_rooms
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load silver stage data into gold dimensional table rooms'
  QUERY_TAG = 'load_sil_stage_to_gold_table_rooms'
  AFTER rentlok_prd.silver.task_stream_to_stage_load_rooms
AS
BEGIN
    MERGE INTO rentlok_prd.gold.dim_rooms AS tgt
    USING (
      SELECT
        sr.room_id,
        sr.property_id,
        sr.room_no,
        sr.floor_no,
        sr.operational_status,
        sr.room_type,
        sr.rent_per_month,
        sr.is_active AS is_active,
        sr.updated_at      AS src_updated_at
      FROM RENTLOK_PRD.SILVER.stage_rooms sr
    ) AS src
      ON tgt.room_id = src.room_id
         AND tgt.is_current = TRUE
    WHEN MATCHED
      AND (
           src.property_id          <> tgt.property_id
        OR src.room_no              <> tgt.room_no
        OR src.floor_no             <> tgt.floor_no
        OR src.operational_status   <> tgt.operational_status
        OR src.room_type            <> tgt.room_type
        OR src.rent_per_month       <> tgt.rent_per_month
        OR src.is_active            <> tgt.is_active
      )
    THEN
      UPDATE SET
        end_date    = CURRENT_DATE() - 1,
        is_current  = FALSE,
        updated_at  = CURRENT_TIMESTAMP();
    
    MERGE INTO rentlok_prd.gold.dim_rooms AS tgt
    USING (
      SELECT
        sr.room_id,
        sr.property_id,
        sr.room_no,
        sr.floor_no,
        sr.operational_status,
        sr.room_type,
        sr.rent_per_month,
        sr.is_active AS is_active
      FROM RENTLOK_PRD.SILVER.stage_rooms sr
    ) AS src
      ON tgt.room_id = src.room_id
         AND tgt.is_current = TRUE
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
        start_date,
        end_date,
        is_current,
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
        CURRENT_DATE(),        
        NULL,                  
        TRUE,                  
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;


--REQUESTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_dimension_table_load_requests
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load silver stage data into gold dimensional table requests'
  QUERY_TAG = 'load_sil_stage_to_gold_table_requests'
  AFTER rentlok_prd.silver.task_stream_to_stage_load_requests
AS
BEGIN
    MERGE INTO rentlok_prd.gold.dim_requests AS tgt
    USING (
      SELECT
        sr.request_id,
        sr.property_id,
        sr.tenant_name         AS request_tenant_name,
        sr.phone_no            AS request_tenant_phno,
        sr.details             AS request_details,
        sr.request_date,
        sr.is_active AS is_active,
        sr.updated_at          AS src_updated_at
      FROM RENTLOK_PRD.SILVER.stage_requests sr
    ) AS src
      ON tgt.request_id = src.request_id
         AND tgt.is_current = TRUE
    WHEN MATCHED
      AND (
           src.property_id             <> tgt.property_id
        OR src.request_tenant_name     <> tgt.request_tenant_name
        OR src.request_tenant_phno     <> tgt.request_tenant_phno
        OR src.request_details         <> tgt.request_details
        OR src.request_date            <> tgt.request_date
        OR src.is_active               <> tgt.is_active
      )
    THEN
      UPDATE SET
        end_date    = CURRENT_DATE() - 1,
        is_current  = FALSE,
        updated_at  = CURRENT_TIMESTAMP();
    
    MERGE INTO rentlok_prd.gold.dim_requests AS tgt
    USING (
      SELECT
        sr.request_id,
        sr.property_id,
        sr.tenant_name         AS request_tenant_name,
        sr.phone_no            AS request_tenant_phno,
        sr.details             AS request_details,
        sr.request_date,
        sr.is_active AS is_active
      FROM RENTLOK_PRD.SILVER.stage_requests sr
    ) AS src
      ON tgt.request_id = src.request_id
         AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN
      INSERT (
        request_id,
        property_id,
        request_tenant_name,
        request_tenant_phno,
        request_details,
        request_date,
        is_active,
        start_date,
        end_date,
        is_current,
        inserted_at,
        updated_at
      )
      VALUES (
        src.request_id,
        src.property_id,
        src.request_tenant_name,
        src.request_tenant_phno,
        src.request_details,
        src.request_date,
        src.is_active,
        CURRENT_DATE(),        
        NULL,                   
        TRUE,                   
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;


--TENANTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_stage_to_dimension_table_load_tenants
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load silver stage data into gold dimensional table tenants'
  QUERY_TAG = 'load_sil_stage_to_gold_table_tenants'
  AFTER rentlok_prd.silver.task_stream_to_stage_load_tenants
AS
BEGIN
    MERGE INTO rentlok_prd.gold.dim_tenants AS tgt
    USING (
      SELECT
        sr.tenant_id,
        sr.tenant_name,
        sr.phone_no as tenant_phone_no, 
        sr.details as tenant_details,
        sr.is_active AS src_is_active
      FROM RENTLOK_PRD.SILVER.stage_tenants sr
    ) AS src
      ON tgt.tenant_id = src.tenant_id
         AND tgt.is_current = TRUE
    WHEN MATCHED
      AND (
           src.tenant_name   <> tgt.tenant_name
        OR src.tenant_phone_no      <> tgt.tenant_phone_no
        OR src.tenant_details       <> tgt.tenant_details
        OR src.src_is_active <> tgt.is_active
      )
    THEN
      UPDATE SET
        end_date    = CURRENT_DATE() - 1,
        is_current  = FALSE,
        updated_at  = CURRENT_TIMESTAMP();
    
    MERGE INTO rentlok_prd.gold.dim_tenants AS tgt
    USING (
      SELECT
        sr.tenant_id,
        sr.tenant_name,
        sr.phone_no as tenant_phone_no, 
        sr.details as tenant_details,
        sr.is_active AS src_is_active
      FROM RENTLOK_PRD.SILVER.stage_tenants sr
    ) AS src
      ON tgt.tenant_id = src.tenant_id
         AND tgt.is_current = TRUE
    WHEN NOT MATCHED THEN
      INSERT (
        tenant_id,
        tenant_name,
        tenant_phone_no,
        tenant_details,
        is_active,
        start_date,
        end_date,
        is_current,
        inserted_at,
        updated_at
      )
      VALUES (
        src.tenant_id,
        src.tenant_name,
        src.tenant_phone_no,
        src.tenant_details,
        src.src_is_active,
        CURRENT_DATE(),        
        NULL,                  
        TRUE,                 
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;


--BOOKINGS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_dimensions_to_fact_table_load_bookings
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load dimensional tables data into fact table bookings'
  QUERY_TAG = 'load_dims_table_to_fact_table_bookings'
  AFTER rentlok_prd.silver.task_stage_to_dimension_table_load_properties,
        rentlok_prd.silver.task_stage_to_dimension_table_load_rooms,
        rentlok_prd.silver.task_stage_to_dimension_table_load_requests,
        rentlok_prd.silver.task_stage_to_dimension_table_load_tenants,
        rentlok_prd.silver.task_stage_to_table_load_bookings
AS
BEGIN
    MERGE INTO rentlok_prd.gold.fact_bookings AS tgt
    USING (
      SELECT
        b.booking_id,
        r.sk_rooms           AS sk_rooms,
        p.sk_properties     AS sk_properties,
        t.sk_tenants         AS sk_tenants,
        d_in.date_key       AS sk_move_in_date,
        d_out.date_key      AS sk_move_out_date,
        b.status,
        DATEDIFF('day', b.move_in_date, b.move_out_date) AS duration_days,
        r.rent_per_month * DATEDIFF('month', b.move_in_date, b.move_out_date) AS revenue_amount
      FROM RENTLOK_PRD.SILVER.bookings b
      JOIN RENTLOK_PRD.GOLD.dim_rooms       r   ON b.room_id     = r.room_id     AND r.is_current
      JOIN RENTLOK_PRD.GOLD.dim_properties  p   ON b.property_id = p.property_id AND p.is_current
      JOIN RENTLOK_PRD.GOLD.dim_tenants     t   ON b.tenant_id   = t.tenant_id   AND t.is_current
      JOIN RENTLOK_PRD.GOLD.dim_date        d_in ON b.move_in_date  = d_in.date
      JOIN RENTLOK_PRD.GOLD.dim_date        d_out ON b.move_out_date = d_out.date
    ) AS src
      ON tgt.booking_id = src.booking_id
    
    WHEN MATCHED THEN
      UPDATE SET
        sk_rooms         = src.sk_rooms,
        sk_properties     = src.sk_properties,
        sk_tenants       = src.sk_tenants,
        sk_move_in_date = src.sk_move_in_date,
        sk_move_out_date= src.sk_move_out_date,
        status          = src.status,
        duration_days   = src.duration_days,
        revenue_amount  = src.revenue_amount,
        updated_at      = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
      INSERT (
        booking_id,
        sk_rooms,
        sk_properties,
        sk_tenants,
        sk_move_in_date,
        sk_move_out_date,
        status,
        duration_days,
        revenue_amount,
        inserted_at,
        updated_at
      )
      VALUES (
        src.booking_id,
        src.sk_rooms,
        src.sk_properties,
        src.sk_tenants,
        src.sk_move_in_date,
        src.sk_move_out_date,
        src.status,
        src.duration_days,
        src.revenue_amount,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;


--PAYMENTS:
CREATE OR REPLACE TASK rentlok_prd.silver.task_dimensions_to_fact_table_load_payments
  WAREHOUSE = COMPUTE_WH
  COMMENT = 'Load dimensional tables data into fact table payments'
  QUERY_TAG = 'load_dims_table_to_fact_table_payments'
  AFTER rentlok_prd.silver.task_dimensions_to_fact_table_load_bookings,
        rentlok_prd.silver.task_stage_to_table_load_payments
AS
BEGIN
    MERGE INTO rentlok_prd.gold.fact_payments AS tgt
    USING (
      SELECT
        p.payment_id,
        fb.sk_bookings            AS sk_bookings,
        d.date_key               AS sk_payment_date,
        p.payment_type,
        p.payment_status,
        p.payment_month,
        p.amount,
        p.updated_at             AS src_updated_at
      FROM RENTLOK_PRD.SILVER.payments p
      JOIN RENTLOK_PRD.GOLD.fact_bookings fb ON p.booking_id = fb.booking_id
      JOIN RENTLOK.GOLD.dim_date d           ON p.payment_date = d.date
    ) AS src
      ON tgt.payment_id = src.payment_id
    
    WHEN MATCHED THEN
      UPDATE SET
        sk_bookings       = src.sk_bookings,
        sk_payment_date  = src.sk_payment_date,
        payment_type     = src.payment_type,
        payment_status   = src.payment_status,
        payment_month    = src.payment_month,
        amount           = src.amount,
        updated_at       = CURRENT_TIMESTAMP()
    
    WHEN NOT MATCHED THEN
      INSERT (
        payment_id,
        sk_bookings,
        sk_payment_date,
        payment_type,
        payment_status,
        payment_month,
        amount,
        inserted_at,
        updated_at
      )
      VALUES (
        src.payment_id,
        src.sk_bookings,
        src.sk_payment_date,
        src.payment_type,
        src.payment_status,
        src.payment_month,
        src.amount,
        CURRENT_TIMESTAMP(),
        CURRENT_TIMESTAMP()
      );
END;




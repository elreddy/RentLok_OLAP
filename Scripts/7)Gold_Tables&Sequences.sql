-- Gold layer Dimensional and Fact tables and Sequences creation:
--=======================================================================

CREATE OR REPLACE SEQUENCE rentlok_prd.gold.dim_properties_seq
  START = 1
  INCREMENT = 1;

CREATE OR REPLACE TABLE rentlok_prd.gold.dim_properties (
  sk_properties      NUMBER           NOT NULL DEFAULT rentlok_prd.gold.dim_properties_seq.NEXTVAL,  
  property_id        NUMBER           NOT NULL,                                   
  property_name      VARCHAR(255)     NOT NULL,                                    
  property_address   VARCHAR(500)     NOT NULL,                                   
  no_of_rooms        NUMBER           NOT NULL,                                    
  is_active          NUMBER           NOT NULL,                      
  start_date         DATE             NOT NULL,                                    
  end_date           DATE             NULL,                                        
  is_current         BOOLEAN          NOT NULL DEFAULT TRUE,                      
  inserted_at        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),        
  updated_at         TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),        
  CONSTRAINT pk_dim_properties PRIMARY KEY (sk_properties),
  CONSTRAINT uq_dim_properties  UNIQUE (property_id, start_date)
)
CLUSTER BY (property_id, start_date)
COMMENT = 'SCD Type II dimension on properties';


CREATE OR REPLACE SEQUENCE rentlok_prd.gold.dim_rooms_seq
  START = 1
  INCREMENT = 1;

CREATE OR REPLACE TABLE rentlok_prd.gold.dim_rooms (
  sk_rooms            NUMBER           NOT NULL DEFAULT rentlok_prd.gold.dim_rooms_seq.NEXTVAL,  
  room_id             NUMBER           NOT NULL,                              
  property_id         NUMBER           NOT NULL,                             
  room_no             VARCHAR(50)      NOT NULL,                             
  floor_no            NUMBER           NOT NULL,                             
  operational_status  VARCHAR(50)      NOT NULL,                             
  room_type           VARCHAR(50)      NOT NULL,                              
  rent_per_month      FLOAT            NOT NULL,                               
  is_active           NUMBER           NOT NULL,                               
  start_date          DATE             NOT NULL,                              
  end_date            DATE             NULL,                                   
  is_current          BOOLEAN          NOT NULL DEFAULT TRUE,                
  inserted_at         TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),   
  updated_at          TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),   
  CONSTRAINT pk_dim_rooms PRIMARY KEY (sk_rooms),
  CONSTRAINT uq_dim_rooms UNIQUE (room_id, start_date)
)
CLUSTER BY (room_id, start_date)
COMMENT = 'SCD Type II dimension on rooms';


CREATE OR REPLACE SEQUENCE rentlok_prd.gold.dim_requests_seq
  START = 1
  INCREMENT = 1;

 CREATE OR REPLACE TABLE rentlok_prd.gold.dim_requests (
  sk_requests           NUMBER           NOT NULL DEFAULT rentlok_prd.gold.dim_requests_seq.NEXTVAL,  
  request_id            NUMBER           NOT NULL,                                      
  property_id           NUMBER           NOT NULL,                                      
  request_tenant_name   VARCHAR(100)     NOT NULL,                                      
  request_tenant_phno   VARCHAR(20)      NOT NULL,                                      
  request_details       VARCHAR(200)     NOT NULL,                                      
  request_date          DATE             NOT NULL,
  is_active             NUMBER           NOT NULL,                           
  start_date            DATE             NOT NULL,              
  end_date              DATE             NULL,            
  is_current            BOOLEAN          NOT NULL DEFAULT TRUE,  
  inserted_at           TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),  
  updated_at            TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),  
  CONSTRAINT pk_dim_requests PRIMARY KEY (sk_requests),
  CONSTRAINT uq_dim_requests UNIQUE (request_id, start_date)
)
CLUSTER BY (request_id, start_date)
COMMENT = 'SCD Type II dimension on tenant requests';


CREATE OR REPLACE TABLE rentlok_prd.gold.dim_date (
  date_key     NUMBER            NOT NULL,     
  date         DATE              NOT NULL,    
  day          NUMBER            NOT NULL,     
  day_of_week  NUMBER            NOT NULL,     
  day_name     VARCHAR(9)        NOT NULL,     
  week         NUMBER            NOT NULL,     
  month        NUMBER            NOT NULL,     
  month_name   VARCHAR(9)        NOT NULL,     
  quarter      NUMBER            NOT NULL,     
  year         NUMBER            NOT NULL,     
  is_weekend   BOOLEAN           NOT NULL,     
  CONSTRAINT pk_dim_date PRIMARY KEY (date_key)
)
CLUSTER BY (date)
COMMENT = 'Date dimension for reporting';

-- Populate the dim_date table with a range of dates
-- Adjust the start date and row count as needed.
INSERT INTO rentlok_prd.gold.dim_date (
  date_key,
  date,
  day,
  day_of_week,
  day_name,
  week,
  month,
  month_name,
  quarter,
  year,
  is_weekend
)
SELECT
  TO_NUMBER(TO_CHAR(DATEADD(day, SEQ4(), '2023-01-01'), 'YYYYMMDD')) AS date_key,
  DATEADD(day, SEQ4(), '2023-01-01')                            AS date,
  EXTRACT(DAY FROM date)                                         AS day,
  EXTRACT(DOW FROM date) + 1                                     AS day_of_week,
  TRIM(TO_CHAR(date, 'Day'))                                     AS day_name,
  EXTRACT(WEEK FROM date)                                        AS week,
  EXTRACT(MONTH FROM date)                                       AS month,
  TRIM(TO_CHAR(date, 'Month'))                                   AS month_name,
  EXTRACT(QUARTER FROM date)                                     AS quarter,
  EXTRACT(YEAR FROM date)                                        AS year,
  CASE WHEN EXTRACT(DOW FROM date) IN (0,6) THEN TRUE ELSE FALSE END AS is_weekend
FROM TABLE(GENERATOR(ROWCOUNT => 1000));


CREATE OR REPLACE SEQUENCE rentlok_prd.gold.dim_tenants_seq
  START = 1
  INCREMENT = 1;

CREATE OR REPLACE TABLE rentlok_prd.gold.dim_tenants (
  sk_tenants        NUMBER           NOT NULL DEFAULT rentlok_prd.gold.dim_tenants_seq.NEXTVAL,  
  tenant_id         NUMBER           NOT NULL,                                       
  tenant_name       VARCHAR(255)     NOT NULL,                                    
  tenant_phone_no   VARCHAR(20)      NOT NULL,                                       
  tenant_details    VARCHAR(500)     NOT NULL,                                       
  is_active         NUMBER           NOT NULL,                         
  start_date        DATE             NOT NULL,                                      
  end_date          DATE             NULL,                                          
  is_current        BOOLEAN          NOT NULL DEFAULT TRUE,                         
  inserted_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),         
  updated_at        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),        
  CONSTRAINT pk_dim_tenants PRIMARY KEY (sk_tenants),
  CONSTRAINT uq_dim_tenants UNIQUE (tenant_id, start_date)
)
CLUSTER BY (tenant_id, start_date)
COMMENT = 'SCD Type II dimension on tenants';


CREATE OR REPLACE SEQUENCE rentlok_prd.gold.fact_bookings_seq
 START = 1
 INCREMENT = 1;

CREATE TABLE RENTLOK_PRD.GOLD.fact_bookings (
  sk_bookings        NUMBER          DEFAULT rentlok_prd.gold.fact_bookings_seq.NEXTVAL,
  booking_id         NUMBER          NOT NULL,
  sk_rooms           NUMBER          NOT NULL,
  sk_properties      NUMBER          NOT NULL,
  sk_tenants         NUMBER          NOT NULL,
  sk_move_in_date    NUMBER          NOT NULL,
  sk_move_out_date   NUMBER          NOT NULL,
  status            VARCHAR(20)      NOT NULL,
  duration_days     NUMBER           NOT NULL,
  revenue_amount    FLOAT            NOT NULL,
  inserted_at       TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at        TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  CONSTRAINT pk_fact_bookings PRIMARY KEY (sk_bookings),
  CONSTRAINT uq_fact_bookings UNIQUE (booking_id)
)
CLUSTER BY (sk_properties, sk_rooms, sk_move_in_date)
COMMENT = 'Fact table storing one row per booking event, linked to dim_rooms, dim_properties, dim_tenants, and dim_date.';


CREATE OR REPLACE SEQUENCE rentlok_prd.gold.fact_payments_seq
  START = 1
  INCREMENT = 1;

CREATE OR REPLACE TABLE rentlok_prd.gold.fact_payments (
  sk_payments        NUMBER           NOT NULL DEFAULT rentlok_prd.gold.fact_payments_seq.NEXTVAL, 
  payment_id        NUMBER           NOT NULL,                                      
  sk_bookings        NUMBER           NOT NULL,                                      
  sk_payment_date   NUMBER           NOT NULL,                                     
  payment_type      VARCHAR(20)      NOT NULL,                                     
  payment_status    VARCHAR(20)      NOT NULL,                                      
  payment_month     VARCHAR(7)       NOT NULL,                                      
  amount            NUMBER(18,2)     NOT NULL,                                     
  inserted_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),          
  updated_at        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),          
  CONSTRAINT pk_fact_payments PRIMARY KEY (sk_payments),
  CONSTRAINT uq_fact_payments UNIQUE (payment_id)
)
CLUSTER BY (sk_bookings, sk_payment_date)
COMMENT = 'Fact table storing one row per payment event, linked to fact_bookings and dim_date';



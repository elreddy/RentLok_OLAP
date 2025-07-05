-- Silver layer Tables, Streams and Sequences Creation:
--  =========================================================================

CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.properties_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.properties (
  sk_property        NUMBER            NOT NULL DEFAULT RENTLOK_PRD.SILVER.properties_seq.NEXTVAL,
  property_id        NUMBER            NOT NULL,
  property_name      VARCHAR           NOT NULL,
  address            VARCHAR           NOT NULL,
  no_of_rooms        NUMBER            NOT NULL,
  is_active          NUMBER            NOT NULL,                   
  batch_id           VARCHAR           NOT NULL,                 
  inserted_at        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at         TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (property_id, batch_id)
)
CLUSTER BY (property_id);

CREATE OR REPLACE STREAM rentlok_prd.silver.stream_properties
ON TABLE rentlok_prd.silver.properties;

CREATE TABLE RENTLOK_PRD.SILVER.stage_properties (
  sk_property        NUMBER            NOT NULL,
  property_id        NUMBER            NOT NULL,
  property_name      VARCHAR           NOT NULL,
  address            VARCHAR           NOT NULL,
  no_of_rooms        NUMBER            NOT NULL,
  is_active          NUMBER            NOT NULL,                   
  batch_id           VARCHAR           NOT NULL,                 
  inserted_at        TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at         TIMESTAMP_NTZ     NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (property_id, batch_id)
)
CLUSTER BY (property_id);


CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.rooms_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.rooms (
  sk_room             NUMBER           NOT NULL DEFAULT RENTLOK_PRD.SILVER.rooms_seq.NEXTVAL,
  room_id             NUMBER           NOT NULL,                     
  property_id         NUMBER           NOT NULL,                     
  room_no             VARCHAR          NOT NULL,                     
  floor_no            NUMBER           NOT NULL,                     
  operational_status  VARCHAR          NOT NULL,                     
  room_type           VARCHAR          NOT NULL,                     
  rent_per_month      FLOAT            NOT NULL,
  is_active           NUMBER           NOT NULL,                     
  batch_id            VARCHAR          NOT NULL,                     
  inserted_at         TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at          TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (room_id, batch_id)
)
CLUSTER BY (room_id);

CREATE OR REPLACE STREAM rentlok_prd.silver.stream_rooms
ON TABLE rentlok_prd.silver.rooms;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.stage_rooms (
  sk_room             NUMBER           NOT NULL,
  room_id             NUMBER           NOT NULL,                     
  property_id         NUMBER           NOT NULL,                     
  room_no             VARCHAR          NOT NULL,                     
  floor_no            NUMBER           NOT NULL,                     
  operational_status  VARCHAR          NOT NULL,                     
  room_type           VARCHAR          NOT NULL,                     
  rent_per_month      FLOAT            NOT NULL,
  is_active           NUMBER           NOT NULL,                     
  batch_id            VARCHAR          NOT NULL,                     
  inserted_at         TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at          TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (room_id, batch_id)
)
CLUSTER BY (room_id);


CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.requests_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.requests (
  sk_request       NUMBER           NOT NULL DEFAULT RENTLOK_PRD.SILVER.requests_seq.NEXTVAL,
  request_id       NUMBER           NOT NULL,                       
  property_id      NUMBER           NOT NULL,                        
  tenant_name      VARCHAR          NOT NULL,                        
  phone_no         VARCHAR          NOT NULL,                       
  details          VARCHAR          NOT NULL,                        
  request_date     DATE             NOT NULL,                        
  is_active        NUMBER           NOT NULL,                        
  batch_id         VARCHAR          NOT NULL,                        
  inserted_at      TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (request_id, batch_id)
)
CLUSTER BY (request_id);

CREATE OR REPLACE STREAM rentlok_prd.silver.stream_requests
ON TABLE rentlok_prd.silver.requests;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.stage_requests (
  sk_request       NUMBER           NOT NULL,
  request_id       NUMBER           NOT NULL,                       
  property_id      NUMBER           NOT NULL,                        
  tenant_name      VARCHAR          NOT NULL,                        
  phone_no         VARCHAR          NOT NULL,                       
  details          VARCHAR          NOT NULL,                        
  request_date     DATE             NOT NULL,                        
  is_active        NUMBER           NOT NULL,                        
  batch_id         VARCHAR          NOT NULL,                        
  inserted_at      TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (request_id, batch_id)
)
CLUSTER BY (request_id);


CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.tenants_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.tenants (
  sk_tenant       NUMBER           NOT NULL DEFAULT RENTLOK_PRD.SILVER.tenants_seq.NEXTVAL,
  tenant_id       NUMBER           NOT NULL,                        
  tenant_name     VARCHAR          NOT NULL,                        
  phone_no        VARCHAR          NOT NULL,                        
  details         VARCHAR          NOT NULL,                        
  is_active       NUMBER(1,0)      NOT NULL,             
  batch_id        VARCHAR          NOT NULL,                        
  inserted_at     TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at      TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (tenant_id, batch_id)
)
CLUSTER BY (tenant_id);

CREATE OR REPLACE STREAM rentlok_prd.silver.stream_tenants
ON TABLE rentlok_prd.silver.tenants;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.stage_tenants (
  sk_tenant       NUMBER           NOT NULL,
  tenant_id       NUMBER           NOT NULL,                        
  tenant_name     VARCHAR          NOT NULL,                        
  phone_no        VARCHAR          NOT NULL,                        
  details         VARCHAR          NOT NULL,                        
  is_active       NUMBER(1,0)      NOT NULL,             
  batch_id        VARCHAR          NOT NULL,                        
  inserted_at     TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at      TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (tenant_id, batch_id)
)
CLUSTER BY (tenant_id);


CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.bookings_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.bookings (
  sk_booking        NUMBER           NOT NULL DEFAULT RENTLOK_PRD.SILVER.bookings_seq.NEXTVAL,
  booking_id        NUMBER           NOT NULL,                        
  room_id           NUMBER           NOT NULL,                       
  tenant_id         NUMBER           NOT NULL,                       
  property_id       NUMBER           NOT NULL,                       
  move_in_date      DATE             NOT NULL,
  move_out_date     DATE             NOT NULL,
  status            VARCHAR          NOT NULL,                        
  is_active         NUMBER(1,0)      NOT NULL,             
  batch_id          VARCHAR          NOT NULL,                        
  inserted_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (booking_id, batch_id)
)
CLUSTER BY (booking_id);


CREATE OR REPLACE SEQUENCE RENTLOK_PRD.SILVER.payments_seq
    START WITH 1
    INCREMENT BY 1;

CREATE OR REPLACE TABLE RENTLOK_PRD.SILVER.payments (
  sk_payment        NUMBER           NOT NULL DEFAULT RENTLOK_PRD.SILVER.payments_seq.NEXTVAL,
  payment_id        NUMBER           NOT NULL,                      
  booking_id        NUMBER           NOT NULL,                        
  payment_type      VARCHAR          NOT NULL,                        
  payment_status    VARCHAR          NOT NULL,                        
  amount            FLOAT            NOT NULL,                        
  payment_date      DATE             NOT NULL,
  payment_month     VARCHAR(7)       NOT NULL,                       
  is_active         NUMBER(1,0)      NOT NULL,             
  batch_id          VARCHAR          NOT NULL,                        
  inserted_at       TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  updated_at        TIMESTAMP_NTZ    NOT NULL DEFAULT CURRENT_TIMESTAMP(),
  UNIQUE (payment_id, batch_id)
)
CLUSTER BY (payment_id);
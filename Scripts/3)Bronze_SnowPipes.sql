--Bronze Layer Snow Pipes Creation:
CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_properties
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.properties
  FROM @rentlok_prd.bronze.s3_rentlok/properties/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_rooms
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.rooms
  FROM @rentlok_prd.bronze.s3_rentlok/rooms/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_requests
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.requests
  FROM @rentlok_prd.bronze.s3_rentlok/requests/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_tenants
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.tenants
  FROM @rentlok_prd.bronze.s3_rentlok/tenants/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_bookings
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.bookings
  FROM @rentlok_prd.bronze.s3_rentlok/bookings/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

CREATE OR REPLACE PIPE rentlok_prd.bronze.pipe_load_payments
  AUTO_INGEST = TRUE
AS
  COPY INTO rentlok_prd.bronze.payments
  FROM @rentlok_prd.bronze.s3_rentlok/payments/
  FILE_FORMAT = my_csv_format
  ON_ERROR = 'CONTINUE';

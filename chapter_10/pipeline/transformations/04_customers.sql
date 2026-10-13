CREATE OR REFRESH STREAMING TABLE st_customer_changes
TBLPROPERTIES ("quality" = "bronze")
AS
  SELECT
    current_timestamp() AS processing_time,
    _metadata.file_path AS source_file_name,
    *
  FROM STREAM read_files(
    "/Volumes/workspace/default/chapter_10/customer_changes/",
    format => "json"
  );

CREATE TEMPORARY VIEW vw_customer_changes
AS
  SELECT
    CAST(customer_id AS INT) AS customer_id,
    customer_name,
    customer_city,
    cdc_change_type,
    CAST(cdc_change_timestamp AS TIMESTAMP) AS cdc_change_timestamp
  FROM STREAM(st_customer_changes);

CREATE OR REFRESH STREAMING TABLE customers
TBLPROPERTIES ("quality" = "silver");

CREATE FLOW customers_flow
AS AUTO CDC INTO customers
  FROM STREAM(vw_customer_changes)
  KEYS (customer_id)
  APPLY AS DELETE WHEN cdc_change_type = 'DELETE'
  SEQUENCE BY cdc_change_timestamp
  COLUMNS * EXCEPT (cdc_change_type, cdc_change_timestamp)
  STORED AS SCD TYPE 1;

CREATE OR REFRESH STREAMING TABLE customers_history
TBLPROPERTIES ("quality" = "silver");

CREATE FLOW customers_history_flow
AS AUTO CDC INTO customers_history
  FROM STREAM(vw_customer_changes)
  KEYS (customer_id)
  APPLY AS DELETE WHEN cdc_change_type = 'DELETE'
  SEQUENCE BY cdc_change_timestamp
  COLUMNS * EXCEPT (cdc_change_type, cdc_change_timestamp)
  STORED AS SCD TYPE 2;

CREATE OR REFRESH STREAMING TABLE st_ip_access_all (
  processing_time TIMESTAMP,
  source_file_name STRING,
  access_date STRING,
  ip_address STRING,
  access_point STRING,
  origin STRING
)
TBLPROPERTIES ("quality" = "bronze");

CREATE FLOW ip_access_web_flow
AS INSERT INTO st_ip_access_all BY NAME
  SELECT
    current_timestamp() AS processing_time,
    _metadata.file_path AS source_file_name,
    access_date,
    ip_address,
    access_point,
    'WEB' AS origin
  FROM STREAM read_files(
    "/Volumes/workspace/default/chapter_10/ip_access_data/",
    format => "json"
  );

CREATE FLOW ip_access_app_flow
AS INSERT INTO st_ip_access_all BY NAME
  SELECT
    current_timestamp() AS processing_time,
    _metadata.file_path AS source_file_name,
    access_date,
    ip_address,
    access_point,
    'APP' AS origin
  FROM STREAM read_files(
    "/Volumes/workspace/default/chapter_10/ip_access_app/",
    format => "json"
  );

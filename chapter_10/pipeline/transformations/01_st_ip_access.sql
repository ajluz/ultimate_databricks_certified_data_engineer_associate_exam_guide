CREATE OR REFRESH STREAMING TABLE st_ip_access
TBLPROPERTIES ("quality" = "bronze")
AS
  SELECT
    current_timestamp() AS processing_time,
    _metadata.file_path AS source_file_name,
    *
  FROM STREAM read_files(
    "/Volumes/workspace/default/chapter_10/ip_access_data/",
    format => "json"
  );

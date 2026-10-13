CREATE TEMPORARY VIEW vw_ip_access_typed
AS
  SELECT
    CAST(access_date AS TIMESTAMP) AS access_date,
    ip_address,
    access_point
  FROM st_ip_access;

CREATE OR REFRESH MATERIALIZED VIEW mv_access_by_point
TBLPROPERTIES ("quality" = "gold")
AS
  SELECT
    access_point,
    COUNT(*) AS qty_access,
    MIN(access_date) AS first_access,
    MAX(access_date) AS last_access
  FROM vw_ip_access_typed
  GROUP BY access_point;

CREATE OR REFRESH PRIVATE STREAMING TABLE st_ip_access_validation (
  CONSTRAINT valid_ip_address EXPECT (ip_address != '999.999.999.999')
)
AS
  SELECT
    *,
    CASE
      WHEN ip_address = '999.999.999.999' THEN TRUE
      ELSE FALSE
    END AS is_invalid
  FROM STREAM(st_ip_access);

CREATE OR REFRESH STREAMING TABLE st_ip_access_quarantine
TBLPROPERTIES ("quality" = "silver")
AS
  SELECT * FROM STREAM(st_ip_access_validation)
  WHERE is_invalid = TRUE;

CREATE OR REFRESH STREAMING TABLE st_ip_access_clean (
  CONSTRAINT known_access_point EXPECT (access_point != 'unknown') ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
AS
  SELECT * EXCEPT (is_invalid) FROM STREAM(st_ip_access_validation)
  WHERE is_invalid = FALSE;

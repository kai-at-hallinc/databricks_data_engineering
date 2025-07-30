-- Databricks notebook source
-- DBTITLE 1,customer bronze
CREATE OR REFRESH STREAMING TABLE customer_bronze
  COMMENT "Raw data from customer feed"
  TBLPROPERTIES (
    "quality" = "bronze",
    "pipelines.reset.allowed" = false
  )
AS
SELECT
  *,
  current_timestamp() AS processing_time,
  _metadata.file_name AS source_file
FROM STREAM read_files(
  "${source}/customer",
  format => "JSON"
);

-- COMMAND ----------

-- DBTITLE 1,customer clean
CREATE OR REFRESH STREAMING TABLE customer_bronze_clean
  (
    CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE,
    CONSTRAINT valid_operation EXPECT(operation IS NOT NULL) ON VIOLATION DROP ROW,
    CONSTRAINT valid_name EXPECT(name IS NOT NULL or operation = "DELETE"),
    CONSTRAINT valid_address EXPECT(
      (
        address IS NOT NULL
        and city IS NOT NULL
        and state IS NOT NULL
        and zip_code IS NOT NULL
      ) or operation = "DELETE"
    ),
    CONSTRAINT valid_email EXPECT(
      rlike(email, '^([a-zA-Z0-9_\\-\\.]+)@([a-zA-Z0-9_\\-\\.]+)\\.([a-zA-Z]{2,5})$')
      or operation = "DELETE"
    ) ON VIOLATION DROP ROW
  ) 
  COMMENT "Clean raw bronze timestamp and add expectations"  
AS
SELECT
  *,
  cast(from_unixtime(timestamp) AS timestamp) AS timestamp_datetime
FROM STREAM customer_bronze;

-- COMMAND ----------

-- DBTITLE 1,customer silver
CREATE OR REFRESH STREAMING LIVE TABLE customer_silver
  COMMENT "SCD type 2 historical customer data";

APPLY CHANGES INTO customer_silver 
  FROM STREAM customer_bronze_clean 
  KEYS (customer_id)
  APPLY AS DELETE WHEN operation = "DELETE"
  SEQUENCE BY timestamp_datetime
  COLUMNS * EXCEPT (timestamp, operation, _rescued_data)
  STORED AS SCD TYPE 2;

-- COMMAND ----------

-- DBTITLE 1,current customers
CREATE OR REFRESH MATERIALIZED VIEW customer_current
  COMMENT "Current list of active customers"
AS
SELECT
  * EXCEPT (processing_time),
  current_timestamp() AS updated_at
FROM
	customer_silver
WHERE `__END_AT` IS NULL;


-- COMMAND ----------

-- DBTITLE 1,customer count
CREATE OR REFRESH MATERIALIZED VIEW customer_counts_state
	COMMENT "Total active customers per state" 
AS
SELECT
	state,
	COUNT(*) AS customer_count,
	CURRENT_TIMESTAMP() updated_at
FROM
	customer_silver
GROUP BY
	state
;

-- COMMAND ----------

-- DBTITLE 1,subscribed emails
CREATE OR REFRESH MATERIALIZED VIEW subscribed_order_email 
  COMMENT "List of orders with email subscription"
AS
SELECT
  a.customer_id,
  a.order_id,
  b.email
FROM
  orders_silver a
  INNER JOIN customer_silver b
  ON a.customer_id = b.customer_id
WHERE
  notifications = 'Y'

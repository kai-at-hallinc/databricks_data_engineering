-- bronze table
CREATE OR REFRESH STREAMING TABLE orders_bronze
	COMMENT "ingest order JSONs"
	TBLPROPERTIES ("quality" = "bronze", "pipeline.reset.allowed" = FALSE) AS
SELECT
	*,
	CURRENT_TIMESTAMP() AS processing_time,
	_metadata.file_name AS source_file
FROM
	STREAM READ_FILES("${source}/order", format => "JSON");

-- silver table
CREATE OR REFRESH STREAMING TABLE orders_silver
	(
		CONSTRAINT valid_date EXPECT(order_timestamp > "2021-12-25  ") ON VIOLATION DROP ROW,
		CONSTRAINT valid_notification EXPECT(notifications IN ("Y", "N")),
		CONSTRAINT valid_id EXPECT(customer_id IS NOT NULL) ON VIOLATION FAIL UPDATE
	)
	COMMENT "clean silver table"
	TBLPROPERTIES ("quality" = "silver") AS
SELECT
	order_id,
	TIMESTAMP(order_timestamp) AS order_timestamp,
	customer_id,
	notifications
FROM
	STREAM orders_bronze;

-- aggregation view
CREATE OR REPLACE MATERIALIZED VIEW order_by_date
	COMMENT "Aggregate data for analysis"
	TBLPROPERTIES ("quality" = "gold") AS
SELECT
	DATE(order_timestamp) AS order_date,
	COUNT(*) AS total_daily_orders
FROM
	orders_silver
GROUP BY
	DATE(order_timestamp);
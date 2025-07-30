-- Databricks notebook source
USE CATALOG databricks_simulated_retail_customer_data;
USE SCHEMA v01;

SELECT current_catalog(), current_schema();


-- COMMAND ----------

SELECT
  *
FROM
  customers
LIMIT 3;

-- COMMAND ----------

-- DBTITLE 1,dynamic view

SELECT
  CASE
    WHEN is_account_group_member('supervisors')
    THEN customer_id
    ELSE 9999999
  END as customer_id,
  * EXCEPT (customer_id)
FROM customers
WHERE
  CASE
    WHEN is_account_group_member('supervisors')
    THEN TRUE
    ELSE loyalty_segment < 3
  END
;

-- COMMAND ----------

-- DBTITLE 1,row filter
CREATE OR REPLACE FUNCTION test_catalog.default.loyalty_row_filter(loyalty_segment STRING)
RETURNS boolean
RETURN IF(is_account_group_member('supervisors'), true, loyalty_segment < 3);

CREATE OR REPLACE FUNCTION test_catalog.default.redact_customer_id(customer_id BIGINT)
RETURN CASE
    WHEN is_account_group_member('supervisors')
    THEN customer_id
    ELSE 9999999
END;

CREATE OR REPLACE TABLE test_catalog.default.customers_filter_and_mask AS
SELECT
  *
FROM customers;

-- ser filter
ALTER TABLE test_catalog.default.customers_filter_and_mask
SET ROW FILTER test_catalog.default.loyalty_row_filter ON (loyalty_segment);

--set mask
ALTER TABLE test_catalog.default.customers_filter_and_mask
  ALTER COLUMN customer_id
  SET MASK test_catalog.default.redact_customer_id;

-- COMMAND ----------

SELECT * FROM  test_catalog.default.customers_filter_and_mask;

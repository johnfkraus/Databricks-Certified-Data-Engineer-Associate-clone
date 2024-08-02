-- Databricks notebook source
-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

-- MAGIC %run ../Includes/Copy-Datasets

-- COMMAND ----------

DROP TABLE IF EXISTS orders;

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

SELECT * FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Overwriting Tables

-- COMMAND ----------

CREATE OR REPLACE TABLE orders AS
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC benefits to overwriting tables instead of deleting and recreating tables
-- MAGIC
-- MAGIC the old version of the table still exists and can easily retrieve all data using Time Travel.
-- MAGIC
-- MAGIC overwriting a table is much faster because it does not need to list the directory recursively or delete any files.
-- MAGIC
-- MAGIC it's an atomic operation.  Concurrent queries can still read the table while you are overwriting it.
-- MAGIC
-- MAGIC due to the ACID transaction guarantees, if overwriting the table fails, the table will be in its previous state.
-- MAGIC
-- MAGIC The first method to accomplish complete overwrite is to use CREATE OR REPLACE TABLE, Also known as CRAS statement.
-- MAGIC
-- MAGIC CREATE OR REPLACE TABLE statements fully replace the content of a table each time they execute.
-- MAGIC
-- MAGIC check our table history, the version 0 is a CREATE TABLE AS SELECT statement.
-- MAGIC
-- MAGIC While, CREATE OR REPLACE statement has generated a new table version.
-- MAGIC
-- MAGIC The second method to overwrite table data is to use INSERT OVERWRITE statement.
-- MAGIC
-- MAGIC It provides a nearly identical output as above.
-- MAGIC
-- MAGIC It means data in the target table will be replaced by data from the query.
-- MAGIC
-- MAGIC However, INSERT OVERWRITE statement has some differences.
-- MAGIC
-- MAGIC For example, it can only overwrite an existing table and not creating a new one like our CREATE OR REPLACE statement.
-- MAGIC
-- MAGIC And it can override only the new records that match the current table schema, which means that it is a safer technique for overwriting an existing table without the risk of modifying the table schema.
-- MAGIC
-- MAGIC we have successfully overwriting the table data and rewriting 2150 records.
-- MAGIC
-- MAGIC And we can see our table history again.
-- MAGIC
-- MAGIC As you can see here, the INSERT OVERWRITE operation has been recorded as a new version in the table as WRITE operation.
-- MAGIC
-- MAGIC And it has the mood "Overwrite".
-- MAGIC
-- MAGIC And if you try to insert overwrite the data with different schema, for example, here we are adding
-- MAGIC
-- MAGIC a new column of the data for the current timestamp.
-- MAGIC
-- MAGIC By running this command, we see that it generates an exception.
-- MAGIC
-- MAGIC And the exception says a schema mismatch detected when writing to the Delta table.
-- MAGIC
-- MAGIC So the way how they enforce schema on-write is the primary difference between INSERT OVERWRITE and
-- MAGIC
-- MAGIC CREATE OR REPLACE TABLE statements.
-- MAGIC
-- MAGIC Let us now talk about appending records to tables.
-- MAGIC
-- MAGIC The easiest method is to use INSERT INTO statement.
-- MAGIC
-- MAGIC Here we are inserting new data using an input query that query the parquet files in the orders-new directory.
-- MAGIC
-- MAGIC We have successfully added 700 new records to our table.
-- MAGIC
-- MAGIC And we can check the new number of orders.
-- MAGIC
-- MAGIC Now we have 2850 records in the orders table.
-- MAGIC
-- MAGIC The INSERT INTO statement is a simple and efficient operation for inserting new data.

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT * FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

DESCRIBE HISTORY orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC And if you try to insert overwrite the data with different schema, for example, here we are adding
-- MAGIC
-- MAGIC a new column of the data for the current timestamp.
-- MAGIC
-- MAGIC By running this command, we see that it generates an exception.
-- MAGIC
-- MAGIC And the exception says a schema mismatch detected when writing to the Delta table.
-- MAGIC
-- MAGIC So the way how they enforce schema on-write is the primary difference between INSERT OVERWRITE and
-- MAGIC
-- MAGIC CREATE OR REPLACE TABLE statements.
-- MAGIC
-- MAGIC Let us now talk about appending records to tables.
-- MAGIC
-- MAGIC The easiest method is to use INSERT INTO statement.
-- MAGIC
-- MAGIC Here we are inserting new data using an input query that query the parquet files in the orders-new

-- COMMAND ----------

INSERT OVERWRITE orders
SELECT *, current_timestamp() FROM parquet.`${dataset.bookstore}/orders`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Appending Data
-- MAGIC
-- MAGIC Can insert duplicate records.

-- COMMAND ----------

INSERT INTO orders
SELECT * FROM parquet.`${dataset.bookstore}/orders-new`

-- COMMAND ----------

SELECT count(*) FROM orders

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Merging Data

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW customers_updates AS 
SELECT * FROM json.`${dataset.bookstore}/customers-json-new`;

MERGE INTO customers c
USING customers_updates u
ON c.customer_id = u.customer_id
WHEN MATCHED AND c.email IS NULL AND u.email IS NOT NULL THEN
  UPDATE SET email = u.email, updated = u.updated
WHEN NOT MATCHED THEN INSERT *

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW books_updates
   (book_id STRING, title STRING, author STRING, category STRING, price DOUBLE)
USING CSV
OPTIONS (
  path = "${dataset.bookstore}/books-csv-new",
  header = "true",
  delimiter = ";"
);

SELECT * FROM books_updates

-- COMMAND ----------

MERGE INTO books b
USING books_updates u
ON b.book_id = u.book_id AND b.title = u.title
WHEN NOT MATCHED AND u.category = 'Computer Science' THEN 
  INSERT *

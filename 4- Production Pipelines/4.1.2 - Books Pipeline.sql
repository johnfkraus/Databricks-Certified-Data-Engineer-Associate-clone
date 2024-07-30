-- Databricks notebook source
SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Bronze Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_bronze
COMMENT "The raw books data, ingested from CDC feed"
AS SELECT * FROM cloud_files("${datasets.path}/books-cdc", "json")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE books_silver;

APPLY CHANGES INTO LIVE.books_silver
  FROM STREAM(LIVE.books_bronze)
  KEYS (book_id)
  APPLY AS DELETE WHEN row_status = "DELETE"
  SEQUENCE BY row_time
  COLUMNS * EXCEPT (row_status, row_time)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Gold Layer Tables
-- MAGIC
-- MAGIC At the gold layer we define a simple aggregate query to create a live table from the data in our book_silver table.
-- MAGIC
-- MAGIC this is not a streaming table.
-- MAGIC
-- MAGIC Since data is being updated and deleted from our book_silver table, it is no more valid to be a streaming source for this new table.
-- MAGIC
-- MAGIC Remember streaming sources must be append only tables.

-- COMMAND ----------

CREATE LIVE TABLE author_counts_state
  COMMENT "Number of books per author"
AS SELECT author, count(*) as books_count, current_timestamp() updated_time
  FROM LIVE.books_silver
  GROUP BY author

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## DLT Views
-- MAGIC
-- MAGIC
-- MAGIC Lastly, we see here how to define a DLT view.
-- MAGIC
-- MAGIC In DLT pipelines,
-- MAGIC
-- MAGIC we can also define views. To define a view,
-- MAGIC
-- MAGIC Simply replace TABLE with the VIEW keyword.
-- MAGIC
-- MAGIC DLT views are temporary views scoped to the DLT pipeline they are a part of, so they are not persisted to the metastore.
-- MAGIC
-- MAGIC Views can still be used to enforce data equality. And metrics for views will be collected and reported as they would be for tables.
-- MAGIC
-- MAGIC Here, we see how we can join and reference tables across notebooks.
-- MAGIC
-- MAGIC We are joining our book_silver table to the orders_cleaned Table, which we created in another notebook in the last lecture.
-- MAGIC
-- MAGIC Since DLT supports scheduling multiple notebooks as part of a single DLT pipeline configuration,  code in any notebook can reference tables and the views created in any other notebook.
-- MAGIC
-- MAGIC Essentially, you can think of the scope of the schema referenced by the LIVE keyword to be at the Delta pipeline level rather than the individual notebook.

-- COMMAND ----------

CREATE LIVE VIEW books_sales
  AS SELECT b.title, o.quantity
    FROM (
      SELECT *, explode(books) AS book 
      FROM LIVE.orders_cleaned) o
    INNER JOIN LIVE.books_silver b
    ON o.book.book_id = b.book_id;

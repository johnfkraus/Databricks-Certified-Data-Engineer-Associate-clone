# Databricks notebook source
# MAGIC %md
# MAGIC The storage location contains four directories: auto loader, checkpoints, system, and tables.
# MAGIC
# MAGIC The system directory captures all the events associated with the pipeline.
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore")
display(files)

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/system/events")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC in the system directory, the event logs are stored as a delta table that you can query to see the events we see in the UI are stored in this data table.
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM delta.`dbfs:/mnt/demo/dlt/demo_bookstore/system/events`

# COMMAND ----------

# MAGIC %md
# MAGIC the tables directory in the storage location contains the five DLT tables.
# MAGIC
# MAGIC

# COMMAND ----------

files = dbutils.fs.ls("dbfs:/mnt/demo/dlt/demo_bookstore/tables")
display(files)

# COMMAND ----------

# MAGIC %md
# MAGIC Get the database name from our pipeline.  If you click on any table, you can see the metastore information.
# MAGIC
# MAGIC copy this information to query a table.
# MAGIC
# MAGIC write a SELECT query on our table using the metadata store information.
# MAGIC
# MAGIC The table exists in the metastore.
# MAGIC
# MAGIC And we can see here the 123 records exist in the "demo_bookstore_dlt_db.cn_daily_customer_books" gold table.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.cn_daily_customer_books

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM demo_bookstore_dlt_db.fr_daily_customer_books

# COMMAND ----------

# MAGIC %md
# MAGIC Finally turn off our the cluster.  Navigate to the compute tab in the left side bar.  Click on the job clusters tab.  terminate this pipeline cluster.
# MAGIC

-- Databricks notebook source
-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC # Delta Live Tables
-- MAGIC
-- MAGIC Delta Live Tables (DLT) is a framework for building reliable and maintainable data processing pipelines.
-- MAGIC
-- MAGIC DLT simplifies the building large scale ETL while maintaining table dependencies and data quality.

-- COMMAND ----------

-- MAGIC %md-sandbox
-- MAGIC
-- MAGIC <div  style="text-align: center; line-height: 0; padding-top: 9px;">
-- MAGIC   <img src="https://raw.githubusercontent.com/derar-alhussein/Databricks-Certified-Data-Engineer-Associate/main/Includes/images/bookstore_schema.png" alt="Databricks Learning" style="width: 600px">
-- MAGIC </div>

-- COMMAND ----------

SET datasets.path=dbfs:/mnt/demo-datasets/bookstore;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ## Bronze Layer Tables
-- MAGIC
-- MAGIC We declare our Delta Live Tables that together implement a simple multi-hop architecture.
-- MAGIC
-- MAGIC DLT tables will always be preceded by the LIVE keyword.
-- MAGIC
-- MAGIC we declare two raw data tables implementing the bronze layer.
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### orders_raw
-- MAGIC
-- MAGIC The table "orders_raw" ingests Parquet data incrementally by Auto Loader from our dataset directory.
-- MAGIC
-- MAGIC Incremental processing via Auto Loader requires the addition of the STREAMING keyword in the declaration.
-- MAGIC
-- MAGIC The "cloud_files()" method enables Auto Loader to be used natively with SQL.
-- MAGIC
-- MAGIC The "cloud_files()" method takes three parameters:
-- MAGIC
-- MAGIC  (1) the data file source location; 
-- MAGIC  
-- MAGIC  (2) the source data format, which is parquet in this case; and 
-- MAGIC  
-- MAGIC  (3) an array of Reader options.  In this case, we declare the schema of our data.
-- MAGIC
-- MAGIC We add a comment that will be visible to anyone exploring the data catalog.
-- MAGIC
-- MAGIC Run the query.  Running a DLT query from here only validates that it is syntactically valid.
-- MAGIC
-- MAGIC To define and populate the table, you must create a DLT pipeline, which we will see later.
-- MAGIC

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_raw
COMMENT "The raw books orders, ingested from orders-raw"
AS SELECT * FROM cloud_files("${datasets.path}/orders-json-raw", "json",
                             map("cloudFiles.inferColumnTypes", "true"))

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### customers
-- MAGIC
-- MAGIC The second bronze table, 'customers', contains JSON-formatted customer data.
-- MAGIC
-- MAGIC The 'customers' table is used below in a join operation to look up customer information.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE customers
COMMENT "The customers lookup table, ingested from customers-json"
AS SELECT * FROM json.`${datasets.path}/customers-json`

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC
-- MAGIC ## Silver Layer Tables
-- MAGIC
-- MAGIC Next, we declare tables implementing the silver layer, which should be a refined copy of data from the bronze layer.
-- MAGIC
-- MAGIC At the silver layer level, we apply operations like data cleansing and enrichment.
-- MAGIC
-- MAGIC #### orders_cleaned
-- MAGIC
-- MAGIC Here we declare our silver table "orders_cleaned", which enriches the order's data with customer information.
-- MAGIC
-- MAGIC In addition, we implement quality control using constraint keywords.  Here we reject records with no order_id.
-- MAGIC
-- MAGIC The Constraint keyword enables DLT to collect metrics on constraint violations.
-- MAGIC
-- MAGIC It provides an optional "ON VIOLATION" clause specifying an action to take on records that violate the constraints.
-- MAGIC
-- MAGIC We need to use the LIVE prefix in order to refer to other DLT tables.
-- MAGIC
-- MAGIC For streaming DTL tables, we need to use the STREAM method.

-- COMMAND ----------

CREATE OR REFRESH STREAMING LIVE TABLE orders_cleaned (
  CONSTRAINT valid_order_number EXPECT (order_id IS NOT NULL) ON VIOLATION DROP ROW
)
COMMENT "The cleaned books orders with valid order_id"
AS
  SELECT order_id, quantity, o.customer_id, c.profile:first_name as f_name, c.profile:last_name as l_name,
         cast(from_unixtime(order_timestamp, 'yyyy-MM-dd HH:mm:ss') AS timestamp) order_timestamp, o.books,
         c.profile:address:country as country
  FROM STREAM(LIVE.orders_raw) o
  LEFT JOIN LIVE.customers c
    ON o.customer_id = c.customer_id

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC The three modes currently supported by Delta are included in the following table.
-- MAGIC
-- MAGIC >> Constraint violation
-- MAGIC
-- MAGIC | **`ON VIOLATION`** | Behavior |
-- MAGIC | --- | --- |
-- MAGIC | **`DROP ROW`** | Discard records that violate constraints |
-- MAGIC | **`FAIL UPDATE`** | Violated constraint causes the pipeline to fail  |
-- MAGIC | Omitted | Records violating constraints will be kept, and reported in metrics |

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC
-- MAGIC ## Gold Tables
-- MAGIC
-- MAGIC The daily number of books in a region.

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE cn_daily_customer_books
COMMENT "Daily number of books per customer in China"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "China"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

CREATE OR REFRESH LIVE TABLE fr_daily_customer_books
COMMENT "Daily number of books per customer in France"
AS
  SELECT customer_id, f_name, l_name, date_trunc("DD", order_timestamp) order_date, sum(quantity) books_counts
  FROM LIVE.orders_cleaned
  WHERE country = "France"
  GROUP BY customer_id, f_name, l_name, date_trunc("DD", order_timestamp)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ### Create a DLT pipeline using this notebook.
-- MAGIC
-- MAGIC Navigate to the Workflows tab on the sidebar.
-- MAGIC
-- MAGIC Select the Delta Live Table tab.
-- MAGIC
-- MAGIC Click Create Pipeline.
-- MAGIC
-- MAGIC ##### General/Pipeline name
-- MAGIC
-- MAGIC Fill in a pipeline name.  For example, "demo bookstore".
-- MAGIC
-- MAGIC ##### General/Pipeline mode
-- MAGIC
-- MAGIC Triggered.
-- MAGIC
-- MAGIC ##### Source code
-- MAGIC
-- MAGIC Paths to notebooks or files that contain pipeline source code.
-- MAGIC
-- MAGIC Use the navigator to locate and select this notebook with the delta tables definitions; i.e.,
-- MAGIC ```
-- MAGIC /Repos/john.f.kraus19.ctr@mail.mil/Databricks-Certified-Data-Engineer-Associate/4- Production Pipelines/4.1 - Delta Live Tables
-- MAGIC ```
-- MAGIC ##### Advanced/Configuration
-- MAGIC
-- MAGIC Under configuration, add a new configuration key/value for the location of the bookstore dataset:
-- MAGIC
-- MAGIC Key: dataset.path
-- MAGIC
-- MAGIC Value: dbfs:/mnt/demo-datasets/bookstore
-- MAGIC
-- MAGIC ##### Destination/Storage location
-- MAGIC
-- MAGIC In the storage location field enter a path where the pipeline logs and data files will be stored, i.e., 
-- MAGIC
-- MAGIC ```dbfs:/mnt/demo/dlt/demo_bookstore```
-- MAGIC
-- MAGIC ##### Destination/Target schema
-- MAGIC
-- MAGIC In the target field, enter a target database name, i.e., "demo_bookstore_dlt_db". 
-- MAGIC
-- MAGIC The pipeline mode specifies how the pipeline will be run.   Select Triggered.
-- MAGIC
-- MAGIC - Triggered pipelines run once and then shut down until the next manual or scheduled updates.
-- MAGIC
-- MAGIC - Continuous pipeline will continuously ingesting new data as it arrives.
-- MAGIC
-- MAGIC A new cluster will be created for our DLT pipeline.
-- MAGIC
-- MAGIC - cluster mode.  For example, fixed size.
-- MAGIC
-- MAGIC - number of workers to zero to create a single node cluster.
-- MAGIC
-- MAGIC Notice below the DBUs estimate provided similar to that provided when configuring interactive clusters.
-- MAGIC
-- MAGIC Finally, click to Create.
-- MAGIC
-- MAGIC #### Run the pipeline
-- MAGIC
-- MAGIC select Development to run the pipeline in development mode.  Development mode allows for interactive development by reusing the cluster, compared to creating a new cluster for each run in the production mode.
-- MAGIC
-- MAGIC development mode also disables retries so that we can quickly identify and fix errors.
-- MAGIC
-- MAGIC click Start.
-- MAGIC
-- MAGIC The initial run will take several minutes while the cluster is provisioned.
-- MAGIC
-- MAGIC On completion, we see an event log for our running pipeline, either information, warning, or errors.
-- MAGIC
-- MAGIC On the right hand side, we see all the pipeline details and also the information related to the cluster.
-- MAGIC
-- MAGIC In the middle, we see the execution flow visualized as a Directed Acyclic Graph (DAG).
-- MAGIC
-- MAGIC This DAG shows the components in the pipeline and the relationships between them.
-- MAGIC
-- MAGIC Click on each component to view a summary which includes the run status and other metadata, including the comment we set during the table definition in the notebook.
-- MAGIC
-- MAGIC We can also see the schema of the table.
-- MAGIC
-- MAGIC If you select the orders_cleaned table you will see the results reported in the data quality section.
-- MAGIC
-- MAGIC Because this flow has data expectation declared those metrics are extracted here.  We should have no records violating our constraint.
-- MAGIC
-- MAGIC #### Add a gold table
-- MAGIC
-- MAGIC open the notebook of this pipeline by clicking on the link in the pipeline.
-- MAGIC
-- MAGIC scroll to the end of this notebook and add a new cell.
-- MAGIC
-- MAGIC add a new table similar to the previous gold table declaration.  But this time, instead of China, we will filter for France.
-- MAGIC
-- MAGIC run the query.  The syntax is valid.
-- MAGIC
-- MAGIC rerun our pipeline.  click start.
-- MAGIC
-- MAGIC Our pipeline is successfully completed and we can see now our two gold tables.
-- MAGIC
-- MAGIC To check what happened under the hood in Delta Live Tables:
-- MAGIC
-- MAGIC the events and information we see in this UI are stored in the storage location we provided during configuring our pipeline: 
-- MAGIC
-- MAGIC ```dbfs:/mnt/demo/dlt/demo_bookstore```
-- MAGIC
-- MAGIC ##### 4.2 - Pipeline Results
-- MAGIC
-- MAGIC To explore the storage location create a Python notebook list the contents of the storage location.
-- MAGIC
-- MAGIC The storage location contains four directories: auto loader, checkpoints, system, and tables.
-- MAGIC
-- MAGIC The system directory captures all the events associated with the pipeline.
-- MAGIC
-- MAGIC in the system directory, the event logs are stored as a delta table that you can query to see the events we see in the UI are stored in this data table.
-- MAGIC
-- MAGIC the tables directory in the storage location contains the five DLT tables.
-- MAGIC
-- MAGIC Get the database name from our pipeline.  If you click on any table, you can see the metastore information.
-- MAGIC
-- MAGIC copy this information to query a table.
-- MAGIC
-- MAGIC write a SELECT query on our table using the metadata store information.
-- MAGIC
-- MAGIC The table exists in the metastore.
-- MAGIC
-- MAGIC And we can see here the 123 records exist in this gold table.
-- MAGIC
-- MAGIC Finally turn off our the cluster.  Navigate to the compute tab in the left side bar.  Click on the job clusters tab.  terminate this pipeline cluster.
-- MAGIC

-- COMMAND ----------



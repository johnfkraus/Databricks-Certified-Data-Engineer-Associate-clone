-- Databricks notebook source
-- MAGIC %md
-- MAGIC spark.databricks.acl.sqlOnly true
-- MAGIC
-- MAGIC https://docs.databricks.com/en/data-governance/table-acls/table-acl.html
-- MAGIC

-- COMMAND ----------

CREATE DATABASE IF NOT EXISTS hr_db
LOCATION 'dbfs:/mnt/demo/hr_db.db';

-- COMMAND ----------

USE hr_db;

CREATE TABLE IF NOT EXISTS employees (id INT, name STRING, salary DOUBLE, city STRING);

INSERT INTO employees
VALUES (1, "Anna", 2500, "Paris"),
       (2, "Thomas", 3000, "London"),
       (3, "Bilal", 3500, "Paris"),
       (4, "Maya", 2000, "Paris"),
       (5, "Sophie", 2500, "London"),
       (6, "Adam", 3500, "London"),
       (7, "Ali", 3000, "Paris");

CREATE VIEW IF NOT EXISTS paris_emplyees_vw
AS SELECT * FROM employees WHERE city = 'Paris';

------------------------------------------------------

-- COMMAND ----------

SHOW GRANts on hive_metastore.hr_db.employees;

-- COMMAND ----------

GRANT SELECT, MODIFY, READ_METADATA, CREATE ON SCHEMA hr_db TO hr_team;

-- COMMAND ----------

GRANT USAGE ON SCHEMA hr_db TO hr_team;

-- COMMAND ----------

GRANT SELECT ON VIEW hr_db.paris_emplyees_vw TO `adam@derar.cloud`;

SHOW GRANTS ON SCHEMA hr_db;

SHOW GRANTS ON VIEW hr_db.paris_emplyees_vw;

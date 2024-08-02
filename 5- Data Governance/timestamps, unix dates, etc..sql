-- Databricks notebook source
-- MAGIC %md
-- MAGIC spark.databricks.acl.sqlOnly true
-- MAGIC
-- MAGIC https://docs.databricks.com/en/data-governance/table-acls/table-acl.html
-- MAGIC

-- COMMAND ----------

-- MAGIC %python
-- MAGIC msPerDay = 24*60*60*1000
-- MAGIC print(24*60*60*1000)
-- MAGIC print(1722530495190 - msPerDay)

-- COMMAND ----------

SELECT unix_millis(current_timestamp()), unix_millis(current_timestamp()) - 86400000

-- COMMAND ----------

SELECT * FROM (
    SELECT 
    userIdentity.email as email, 
    collect_set(
        if(isnotnull(workspaceId), 
        workspaceId, "NONE")
    ) AS workspaceIds, 
    collect_set(serviceName) AS serviceNames, 
    size(collect_set(serviceName)) AS num_services, 
    collect_set(actionName) AS action_names, 
    size(collect_set(actionName)) AS num_actions, 
    count(*) AS num_permissions_changes,
    collect_set(date) as dates
    FROM hive_metastore.default.audit_log_new_messages
      WHERE actionName IN ('addPrincipalToGroup', 'changeDatabricksSqlAcl', 'changeDatabricksWorkspaceAcl', 'changeDbTokenAcl', 'changePasswordAcl', 'changeServicePrincipalAcls', 'generateDbToken', 'setAdmin', 'changeClusterAcl', 'changeClusterPolicyAcl', 'changeWarehouseAcls', 'changePermissions', 'transferObjectOwnership', 'changePipelineAcls', 'changeFeatureTableAcl', 'addPrincipalToGroup', 'changeIamRoleAcl', 'changeInstancePoolAcl', 'changeJobAcl', 'resetJobAcl', 'changeRegisteredModelAcl', 'changeInferenceEndpointAcl', 'putAcl', 'changeSecurableOwner', 'grantPermission', 'changeWorkspaceAcl', 'updateRoleAssignment', 'setAccountAdmin', 'changeAccountOwner', 'updatePermissions', 'updateSharePermissions') 
      -- AND timestamp >= (unix_millis(current_timestamp()) - 86400000 * 7) -- - INTERVAL 24 HOURS ;
      AND userIdentity.email NOT IN ('System-User') 
      GROUP BY 1 )  
WHERE num_permissions_changes > 1 -- 25 
AND email != 'john.f.kraus19.ctr@mail.mil'
ORDER BY num_permissions_changes DESC

-- COMMAND ----------


    SELECT WINDOW(timestamp(timestamp), '60 minutes').start AS window_start, 
    WINDOW(timestamp(timestamp), '60 minutes').end AS window_end,
    INTERVAL 24 HOURS,
    *
    FROM hive_metastore.default.audit_logs limit 10;


-- WHERE timestamp >= unix_millis(current_timestamp())  -- - INTERVAL 24 HOURS ;
  

-- COMMAND ----------

SELECT unix_millis(timestamp(INTERVAL 24 HOURS))

-- COMMAND ----------

SELECT * FROM 
    (
    SELECT WINDOW(timestamp(timestamp), '60 minutes').start AS window_start, 
    WINDOW(timestamp(timestamp), '60 minutes').end AS window_end, 
    userIdentity.email as email, 
    collect_set(
        if(isnotnull(workspaceId), 
        workspaceId, "NONE")
    ) AS workspaceIds, 
    collect_set(serviceName) AS serviceNames, 
    size(collect_set(serviceName)) AS num_services, 
    collect_set(actionName) AS action_names, 
    size(collect_set(actionName)) AS num_actions, 
    count(*) AS num_permissions_changes,

    FROM hive_metastore.default.audit_logs 
    WHERE actionName IN ('addPrincipalToGroup', 'changeDatabricksSqlAcl', 'changeDatabricksWorkspaceAcl', 'changeDbTokenAcl', 'changePasswordAcl', 'changeServicePrincipalAcls', 'generateDbToken', 'setAdmin', 'changeClusterAcl', 'changeClusterPolicyAcl', 'changeWarehouseAcls', 'changePermissions', 'transferObjectOwnership', 'changePipelineAcls', 'changeFeatureTableAcl', 'addPrincipalToGroup', 'changeIamRoleAcl', 'changeInstancePoolAcl', 'changeJobAcl', 'resetJobAcl', 'changeRegisteredModelAcl', 'changeInferenceEndpointAcl', 'putAcl', 'changeSecurableOwner', 'grantPermission', 'changeWorkspaceAcl', 'updateRoleAssignment', 'setAccountAdmin', 'changeAccountOwner', 'updatePermissions', 'updateSharePermissions') 
    AND timestamp(timestamp) >= unix_millis(current_timestamp())   --  - INTERVAL 24 HOURS 
    AND userIdentity.email NOT IN ('System-User') 
    GROUP BY 1, 2, 3
    ) 
WHERE num_permissions_changes > 1 -- 25 
ORDER BY num_permissions_changes DESC

-- COMMAND ----------

SELECT timestamp(unix_millis(current_timestamp()))

-- COMMAND ----------

SELECT unix_millis(current_timestamp())

-- COMMAND ----------

SELECT current_date()

-- COMMAND ----------

SELECT current_timestamp();

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

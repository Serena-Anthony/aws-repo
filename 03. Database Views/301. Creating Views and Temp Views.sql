-- Databricks notebook source
-- MAGIC %md
-- MAGIC ### Creating View

-- COMMAND ----------

select current_metastore();

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC for dirs in dbutils.fs.ls(''):
-- MAGIC     print(dirs.path)
-- MAGIC     print()
-- MAGIC

-- COMMAND ----------

--create catalog 
create catalog if not exists serio_cat;

--set this as default cat
use catalog serio_cat;

--wht is default cat currently
select current_catalog();

--create schema
create schema if not exists serio_cat.serio_schema;

--set this as default schema
use schema serio_schema;

select current_schema();


-- COMMAND ----------

select current_catalog()

-- COMMAND ----------

-- DBTITLE 1,Creating Base Table for Views
CREATE or REPLACE TABLE sales(s_id INT, 
                    product_id STRING, 
                    product_category STRING,
                    emp_id INT,
                    cust_id INT,
                    amount FLOAT);

INSERT INTO sales VALUES(1, 'P100', 'office', 101, 1001, 10000),
                        (2, 'P100', 'office', 101, 1002, 11000),
                        (3, 'P100', 'office', 101, 1003, 12000),
                        (4, 'P101', 'home', 102, 1001, 11500),
                        (5, 'P101', 'home', 102, 1001, 12500),
                        (6, 'P101', 'home', 102, 1001, 10500),
                        (7, 'P102', 'electronics', 103, 1001, 11100),
                        (8, 'P102', 'electronics', 103, 1001, 12100),
                        (9, 'P102', 'electronics', 103, 1001, 13100),
                        (10, 'P103', 'electronics', 104, 1001, 15000)

-- COMMAND ----------

-- DBTITLE 1,See data in table
select * from sales;

-- COMMAND ----------

-- DBTITLE 1,See the list of Tables and Views
show tables;

-- COMMAND ----------

-- DBTITLE 1,Creating simple View
create or replace view electronics_products
as 
select * from sales where product_category = 'electronics'

-- COMMAND ----------

-- DBTITLE 1,See table list
show tables;

-- COMMAND ----------

-- DBTITLE 1,Creating temp view
create or replace temporary view office 
as
select * from sales where product_category = 'office';

-- COMMAND ----------

-- DBTITLE 1,See Tables
show tables;

-- COMMAND ----------

-- DBTITLE 1,Error: We will see this in unity catalog
-- Materialized views can be created only with the Unity catalog enabled workspace
-- stores result of query in physcial table unlike view which are temporary & virtual
CREATE OR REPLACE MATERIALIZED VIEW sales_summary
AS
SELECT product_category, sum(amount) as TOTAL_SALES, avg(amount) as AVG_SALES
    FROM sales
    GROUP BY product_category;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Python Syntax: createOrReplaceTempView

-- COMMAND ----------

-- MAGIC %python
-- MAGIC
-- MAGIC sales_schema = ["s_id", "product_id", "product_category", "emp_id", "cust_id", "amount"]
-- MAGIC
-- MAGIC sales_data = [
-- MAGIC                 (1, 'P100', 'office', 101, 1001, 10000),
-- MAGIC                 (2, 'P100', 'office', 101, 1002, 11000),
-- MAGIC                 (3, 'P100', 'office', 101, 1003, 12000),
-- MAGIC                 (4, 'P101', 'home', 102, 1001, 11500),
-- MAGIC                 (5, 'P101', 'home', 102, 1001, 12500),
-- MAGIC                 (6, 'P101', 'home', 102, 1001, 10500),
-- MAGIC                 (7, 'P102', 'electronics', 103, 1001, 11100),
-- MAGIC                 (8, 'P102', 'electronics', 103, 1001, 12100),
-- MAGIC                 (9, 'P102', 'electronics', 103, 1001, 13100),
-- MAGIC                 (10, 'P103', 'electronics', 104, 1001, 15000)
-- MAGIC ]
-- MAGIC  
-- MAGIC salesDF = spark.createDataFrame(data=sales_data, schema=sales_schema)
-- MAGIC
-- MAGIC salesDF.show()

-- COMMAND ----------

-- DBTITLE 1,Creating Temp view: createOrReplaceTempView()
-- MAGIC %python
-- MAGIC # temp table or temp view created within spark session
-- MAGIC # create temp table out of data frame, which can be used in spark sql
-- MAGIC salesDF.createOrReplaceTempView('salesView')

-- COMMAND ----------

describe salesDF;

-- COMMAND ----------

-- MAGIC %python
-- MAGIC

-- COMMAND ----------

?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC ========================================== END =====================================

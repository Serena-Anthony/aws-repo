-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Understanding Delta Lake Transaction Log
-- MAGIC Understand the cloud storage directory structure behind delta lake tables

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 0. Create a new schema under the demo catalog for this section of the course (delta_lake)

-- COMMAND ----------

create schema if not exists serio_cat.demo_db
  managed location '/Volumes/serio_cat/demo_db/sample_vol/delta_lake/'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 1. Create a Delta Lake Table

-- COMMAND ----------

create table if not exists serio_cat.demo_db.companies
(company_name STRING,
founded_date DATE,
country STRING);

-- COMMAND ----------

-- DBTITLE 1,describe table
desc extended serio_cat.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 2. Insert some data

-- COMMAND ----------

insert into serio_cat.demo_db.companies values ('mitshubushi', '1870-04-01', 'Japan')

-- COMMAND ----------




-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### 3. Insert some more data

-- COMMAND ----------

insert into serio_cat.demo_db.companies 
values ('apple', '1970-04-04', 'USA'),
('Google','1989-09-04','USA'),
('Amazon','1990-07-05','USA')


-- COMMAND ----------



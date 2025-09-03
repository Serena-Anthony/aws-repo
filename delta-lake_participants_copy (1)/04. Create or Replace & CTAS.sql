-- Databricks notebook source
-- MAGIC %md
-- MAGIC ## Create or Replace & CTAS
-- MAGIC 1. Difference between Create or Replace and Drop and Create Table statements
-- MAGIC 2. CTAS statement (crete table using AS)
-- MAGIC
-- MAGIC
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 1. Difference between Create or Replace and Drop and Create Table statements
-- MAGIC

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Behaviour of the DROP and CREATE statements

-- COMMAND ----------

DROP TABLE IF EXISTS serio_cat.demo_db.companies;

CREATE TABLE serio_cat.demo_db.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO serio_cat.demo_db.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

-- COMMAND ----------

DESC HISTORY serio_cat.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #### Behaviour of the CREATE OR REPLACE statement

-- COMMAND ----------

DROP TABLE IF EXISTS sample_catalog.demo_db.companies;

-- COMMAND ----------

CREATE OR REPLACE TABLE serio_cat.demo_db.companies
  (company_id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
   company_name STRING,
   founded_date DATE,
   country      STRING);

INSERT INTO serio_cat.demo_db.companies 
(company_name, founded_date, country)
VALUES ("Apple", "1976-04-01", "USA"),
       ("Microsoft", "1975-04-04", "USA"),
       ("Google", "1998-09-04", "USA"),
       ("Amazon", "1994-07-05", "USA"),
       ("Tencent", "1998-11-11", "China");   

-- COMMAND ----------

DESC HISTORY serio_cat.demo_db.companies;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### 2. CTAS statement

-- COMMAND ----------

CREATE TABLE serio_cat.demo_db.companies_china
AS
SELECT * FROM serio_cat.demo_db.companies
WHERE country = 'China';

-- COMMAND ----------

select * from  serio_cat.demo_db.companies_china

-- COMMAND ----------

DROP TABLE IF EXISTS serio_cat.demo_db.companies_china;

-- COMMAND ----------

CREATE TABLE serio_cat.demo_db.companies_china
AS
SELECT cast(company_id as INT ) as company_id,
      company_name, founded_date, country
      FROM serio_cat.demo_db.companies
WHERE country = 'China';

-- COMMAND ----------

alter table serio_cat.demo_db.companies_china
alter column founded_date comment'date the company was founded'

-- COMMAND ----------

desc serio_cat.demo_db.companies_china

-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------



-- COMMAND ----------

DESC HISTORY sample_catalog.demo_db.companies_china;

-- COMMAND ----------

CREATE OR REPLACE TABLE order_tab
AS
SELECT * FROM csv.`/Volumes/serio_cat/default/order_vol/orders.csv`;

-- COMMAND ----------

select * from order_tab;

-- COMMAND ----------

create or replace table order_tab_new using delta as
select * from read_files('/Volumes/serio_cat/default/order_vol/orders.csv',
  format => 'csv',
  sep => ',',
  header => true,
  mode => 'FAILFAST');


-- COMMAND ----------

SELECT * FROM order_tab_new

-- COMMAND ----------



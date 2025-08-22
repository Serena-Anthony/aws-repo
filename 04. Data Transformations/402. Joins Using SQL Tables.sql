-- Databricks notebook source
-- DBTITLE 1,What is Hive?
-- MAGIC %python
-- MAGIC # hive is Data Warehouse built on top of hadoop

-- COMMAND ----------

-- cleanup

DROP TABLE IF EXISTS products;
DROP TABLE IF EXISTS brands;

-- COMMAND ----------

-- DBTITLE 1,See Available Databases
-- MAGIC %python
-- MAGIC
-- MAGIC df = ?
-- MAGIC df.printSchema()
-- MAGIC df.show()

-- COMMAND ----------

-- DBTITLE 1,Alternate to Above cell
?

-- COMMAND ----------

-- DBTITLE 1,Get the list of Tables and views in default Database
?

-- COMMAND ----------

-- DBTITLE 1,Create Tables in default database
-- it will be managed table, not external table (we will discuss more about them during unity catalog)

?

?

-- COMMAND ----------

-- DBTITLE 1,Confirm Table creation
?

-- COMMAND ----------

-- DBTITLE 1,Insert data into both the tables
-- no matching brand for product "Redmi"
INSERT INTO products VALUES (1, 'iPhone', 100),
                          (2, 'Galaxy', 200),
                          (3, 'Redmi', 300),
                          (4, 'Pixel', 400);


-- no matching products for "sony" brand
INSERT INTO brands VALUES (100, "Apple"),
                          (200, "Samsung"),
                          (400, "Google"),
                          (500, "Sony");

-- COMMAND ----------

-- DBTITLE 1,Create temp views from DFs created in above cell
?

-- COMMAND ----------

-- DBTITLE 1,See brands table
?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC
-- MAGIC #### Types of joins in Spark
-- MAGIC
-- MAGIC - inner join (default) 
-- MAGIC - cross join
-- MAGIC - outer/ full/ fullouter/ full_outer
-- MAGIC - left/ leftouter/ left_outer
-- MAGIC - right/ rightouter/ right_outer
-- MAGIC - semi/ leftsemi/ left_semi
-- MAGIC - anti/ leftanti/ left_anti

-- COMMAND ----------

-- DBTITLE 1,Inner join in SQL
?

-- COMMAND ----------

-- DBTITLE 1,Inner join is Default Join
?

-- COMMAND ----------

-- DBTITLE 1,Full Outer Join
?

-- COMMAND ----------

-- DBTITLE 1,Left Outer join
?

-- COMMAND ----------

-- DBTITLE 1,Right outer join
?

-- COMMAND ----------

-- DBTITLE 1,Left Semi Join
?

-- COMMAND ----------

-- DBTITLE 1,Left Anti Join
?

-- COMMAND ----------

-- DBTITLE 1,Cross join
?

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ======================================== END =========================================

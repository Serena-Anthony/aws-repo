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

show databases;

-- COMMAND ----------

-- DBTITLE 1,Alternate to Above cell
?

-- COMMAND ----------

-- DBTITLE 1,Get the list of Tables and views in default Database
show tables;

-- COMMAND ----------

-- DBTITLE 1,Create Tables in default database
-- it will be managed table, not external table (we will discuss more about them during unity catalog)

create table products(product_id int, product_name string, brand_id int);

create table brands(brand_id int , brand_name string);



-- COMMAND ----------

-- DBTITLE 1,Confirm Table creation
show tables;

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

select * from brands

-- COMMAND ----------

-- DBTITLE 1,Create temp views from DFs created in above cell
create or replace temporary view brand_500
as
select * from brands where brand_id = '500';

-- COMMAND ----------

-- DBTITLE 1,See brands table
select * from brand_500;

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
select products.*, brands.* from products inner join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Inner join is Default Join
select products.*, brands.* from products join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Full Outer Join
select products.*, brands.* from products full outer join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Left Outer join
select products.*, brands.* from products left outer join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Right outer join
select products.*, brands.* from products right outer join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Left Semi Join
select products.*from products left semi join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Left Anti Join
select products.* from products left anti join brands
on products.brand_id = brands.brand_id ;

-- COMMAND ----------

-- DBTITLE 1,Cross join
select products.*, brands.* from products cross join brands ;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ======================================== END =========================================

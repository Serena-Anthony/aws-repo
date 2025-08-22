# Databricks notebook source
# MAGIC %md
# MAGIC
# MAGIC Here we will create 3 different DataFrames using existing spark session

# COMMAND ----------

# DBTITLE 1,Creating productDf DataFrames
products = [ 
          # (product_id, product_name, brand_id)  
         (1, 'iPhone', 100),
         (2, 'Galaxy', 200),
         (3, 'Redme', 300), #   no matching brand
         (4, 'Pixel', 400),
]

productDf = spark.createDataFrame(data=products, schema=["product_id", "product_name", "brand_id"])

productDf.show()

# COMMAND ----------

# DBTITLE 1,Creating brandDf DataFrames
brands = [
    #(brand_id, brand_name)
    (100, "Apple"),
    (200, "Samsung"),
    (400, "Google"),
    (500, "Sony"), # no matching products
]

brandDf = spark.createDataFrame(data=brands, schema=["brand_id", "brand_name"])

brandDf.show()

# COMMAND ----------

store = [
    #(store_id, store_name)
    (1000, "Croma"),
    (2000, "Reliance Digital"),
    (4000, "Amazon"),
    (5000, "Flipkart"), 
]
 
storeDf = spark.createDataFrame(data=store, schema=["store_id", "store_name"])

storeDf.show()

# COMMAND ----------

# DBTITLE 1,Creating another session
# in any spark application, there will be ONLY ONE spark context but we can create as many spark sessions allowed

?


# COMMAND ----------

# DBTITLE 1,Create one Temp view
# we created productDf using spark.createDataFrame
# create product temp table in spark session
# products is temp view, private to spark session, means we cannot access from spark2 session
# productDf = spark.createDataFrame(...), productDf is private to spark session
# temp table created shall go to specific spark session

?

# COMMAND ----------

# DBTITLE 1,See the available tables on session 1 (spark)
?

# COMMAND ----------

# DBTITLE 1,Check if these tables are available in session 2 or not (spark2)
?

# COMMAND ----------

# DBTITLE 1,Try to access data from table with session 1
# now access products from session /knew it will work

?

# COMMAND ----------

# DBTITLE 1,Try to access data from table with session 2
# now try to access products from spark2, IT WILL FAIL, as products table private to spark session

# error  AnalysisException: Table or view not found: products; 

?

# COMMAND ----------

# DBTITLE 1,Creating Global Temp View
# now create global temp view global_temp that can be shared across multiple sessions on same notebook
# spark has a database global_temp [built in]
# create a table 'brands' in global_temp

?

# COMMAND ----------

# DBTITLE 1,Get the list of tables from global_temp database (with spark session)
?

# COMMAND ----------

# DBTITLE 1,Get the list of tables from global_temp database (with spark2 session)

# brands from global temp will be displayed, but products table not listed as it was created for spark session, not for spark2 session

?

# COMMAND ----------

# DBTITLE 1,List of tables in default and local databases
# MAGIC %sql
# MAGIC -- list all the tables which are accessible by all the current sessions (from default database)
# MAGIC ?

# COMMAND ----------

# DBTITLE 1,List of tables in local and global databases
# MAGIC %sql
# MAGIC -- list all temp views which are accessible by all the spark sessions
# MAGIC ?

# COMMAND ----------

# DBTITLE 1,Quick Notes

## TempView : specific to a single spark session and won't be accessible outside that session
## GlobalTempView: can be accessed by all the spark sessions but lifetime is till the driver program is running
## Database tables: permanent till the time your cluster is there

# COMMAND ----------

# DBTITLE 1,Access global temp view using spark session
?

# COMMAND ----------

# DBTITLE 1,Create Products temp view in spark2 session
products = [

    (1000, 'Swift'),
    (1001, 'Nano'),
    (1002, 'Vista'),
    (1004, 'Dzire')
]


productDF2 = spark2.createDataFrame(data=products, schema=["product_id", "product_name"])
productDF2.show()
productDF2.createOrReplaceTempView("products")

# COMMAND ----------

# DBTITLE 1,Describe products view from both sessions
?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ========================================== END ====================================

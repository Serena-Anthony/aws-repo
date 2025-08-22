# Databricks notebook source
# DBTITLE 1,Create dataframes
products_schema = ["product_id", "product_name", "brand_id"]  
products_data = [
                    (101, 'iPhone', 100),
                    (102, 'Galaxy', 200),
                    (103, 'Redme', 300), #   no matching brand
                    (104, 'Pixel', 400),
                ]

brands_schema = ["brand_id", "brand_name"]
brands_data =   [
                    (100, "Apple"),
                    (200, "Samsung"),
                    (400, "Google"),
                    (500, "Sony"), # no matching products
                ]
 
productDf = spark.createDataFrame(data=products_data, schema=products_schema)
brandDf = spark.createDataFrame(data=brands_data, schema=brands_schema)

productDf.show()
brandDf.show()

# COMMAND ----------

# DBTITLE 1,Join Documentation
help(join)

# COMMAND ----------

# DBTITLE 1,Inner Join
# productDf is left
# brandDf is right
# inner join: select/pick only matching record, discard if no matches found
# join returns a new df

df = productDf.join(other=brandDf, on=productDf.brand_id == brandDf.brand_id, how="inner")
df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,Inner join without duplicate columns
# how to remove specific column, when the same column present in two dataframe

from pyspark.sql.functions import col

# using col("brand_id") will have Reference to 'brand_id' it is ambiguous, since brand_id available from 2 dfs
# as men tioned below we have 2 choices to delete brand_id
# drop(brandDf["brand_id"]) drop column from brandDf
# drop(productDf.brand_id) drop column from productDf

df = productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"inner").drop(brandDf["brand_id"])

df.printSchema()
df.show()


# COMMAND ----------

# if col name is same in both tables => kinda like natural join
# without specifying value for how -> default = innerjoin

df = productDf.join(brandDf, on='brand_id',how = "inner")
df.printSchema()
df.show()

# COMMAND ----------

# DBTITLE 1,Full Outer Join
# Outer Join, Full Outer Outer, [Left outer + Right outer]
# pick all records from left dataframe, and also right dataframe
# if no matches found, it fills null data for not matched records
productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"outer").show()


# COMMAND ----------

# DBTITLE 1,Left Outer join
# Left, Left Outer join 
# picks all records from left, if no matches found, it fills null for right data
productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"leftouter").show()

# COMMAND ----------

# DBTITLE 1,Right Outer Join
# Right, Right outer Join
# picks all the records from right, if no matches found, fills left data with null
productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"rightouter").show()


# COMMAND ----------

# DBTITLE 1,Left Semi Join
# join in general convention, it pull the records from both right and left, join them based on condition
# left semi join, join left and right based on condition, however it project the records only from left side

# it is similar to innerjoin, but picks/projects records only from left
# we can't see brand_id, brand_name from brands df
# result will not include not matching data [similar to inner join]

productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"leftsemi").show()

# COMMAND ----------

# DBTITLE 1,Try rightsemi join: Error
# it will give error as rightsemi join does not exist
# also see available types of joins in the error description

productDf.join(brandDf, productDf["brand_id"] ==  brandDf["brand_id"], "rightsemi").show()

# COMMAND ----------

# DBTITLE 1,Left Anti Join
# Exact opposite to semi join
# Picks the records that doesn't have match on the right side

productDf.join(brandDf, productDf.brand_id == brandDf["brand_id"],"leftanti").show()

# COMMAND ----------

# DBTITLE 1,Left Anti join by swaping left and right tables
# as right anti join is not available, we can get the result for the same by swapping dataframes.

brandDf.join(productDf, productDf.brand_id == brandDf["brand_id"],"leftanti").show()

# COMMAND ----------

# DBTITLE 1,Create one more Dataframe
store_schema = ['store_id', 'store_name']

store_data = [
                (1000, "Poorvika"),
                (2000, "Sangeetha"),
                (4000, "Amazon"),
                (5000, "FlipKart"), 
            ]
 
storeDf = spark.createDataFrame(data=store_data, schema=store_schema)
storeDf.show()


# COMMAND ----------

# DBTITLE 1,Cross Join
# cartesian product, take row from left side, pair with all from right side

productDf.crossJoin(storeDf).show()

# COMMAND ----------

df = productDf.join(storeDf, how='cross').show()
#no need condition for cross join

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC =========================================== END ========================================

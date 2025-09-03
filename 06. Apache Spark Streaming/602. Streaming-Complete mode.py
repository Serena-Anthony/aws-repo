# Databricks notebook source
# DBTITLE 1,Cleanup: cleaning Directories
# removing existing data files
# dbutils.fs.rm("/FileStore/orders_data/orders1.csv")
dbutils.fs.rm("/FileStore/orders_data/orders2.csv")
dbutils.fs.rm("/FileStore/orders_data/orders3.csv")
dbutils.fs.rm("/FileStore/orders_data/orders4.csv")

# removing checkpoint directory 
dbutils.fs.rm("/FileStore/checkpoints/order-ingestion-to-table", True)

# COMMAND ----------

# DBTITLE 1,Removing Table
# MAGIC %sql
# MAGIC DROP TABLE IF EXISTS aggregated_orders;

# COMMAND ----------

# DBTITLE 1,Scheam Defination
# define schema for the csv files

schema = """
        order_id STRING,
        amount DOUBLE,
        Category STRING,
        dept STRING
        """

# COMMAND ----------

# DBTITLE 1,Defining source of stream
# Use Autoloader to ingest CSV files

from pyspark.sql.functions import col

input_path = "/FileStore/orders_data/"


df = (spark.readStream.format("cloudfiles")
       .option("cloudFiles.format", "csv")
       .schema(schema)
       .option("cloudFiles.inferColumnTypes", "true")
       .option("cloudFiles.includeExistingFiles", "true")
       .load(input_path))


# COMMAND ----------

# DBTITLE 1,This is streaming dataframe
df.isStreaming

# COMMAND ----------

# DBTITLE 1,See the schema of DF
df.printSchema()

# COMMAND ----------

# DBTITLE 1,We can not use show() function on streaming dataframe
# can't use show function on stream, show is an action functions, and for batch mode 

df.show()  # error

# COMMAND ----------

# DBTITLE 1,Aggregation on data in dataframe
# Example transformation : This time we do not need transaction level data, we need department wise total sales amount

aggregated_df = df.groupBy("dept").sum("amount")
aggregated_df = aggregated_df.withColumnRenamed('sum(amount)', 'total_sales')

# COMMAND ----------

aggregated_df.printSchema()

# COMMAND ----------

# DBTITLE 1,Defining target for streaming
# Define table name
# table will be created automatically by spark in the defualt database
table = 'aggregated_orders'

# write filtered dataframe to the delta table

# checkpoints: it is used for fault tolerance
# checkpoint shall have references of already processed filres, so that will not process the same file again, if it restarted, crashed and restarted, useful for Fault tolerance
# checkpoint the location should be indepedent for every stream

query = (aggregated_df.writeStream
         .format("delta")
         .outputMode("complete")
         .option("checkpointLocation", "/FileStore/checkpoints/order-ingestion-to-table")
         .toTable(table))

# COMMAND ----------

# DBTITLE 1,Check record count in Target Table
# MAGIC %sql
# MAGIC Select count(*) from aggregated_orders;

# COMMAND ----------

# DBTITLE 1,Check data in Target Table
# MAGIC %sql
# MAGIC select * from aggregated_orders;

# COMMAND ----------

# DBTITLE 1,Error: Try update mode
# Define table name
# table will be created automatically by spark in the defualt database
table = 'aggregated_orders'

# write filtered dataframe to the delta table

# checkpoints: it is used for fault tolerance
# checkpoint shall have references of already processed filres, so that will not process the same file again, if it restarted, crashed and restarted, useful for Fault tolerance
# checkpoint the location should be indepedent for every stream

query = (aggregated_df.writeStream
         .format("delta")
         .outputMode("update")
         .option("checkpointLocation", "/FileStore/checkpoints/order-ingestion-to-table")
         .toTable(table))

# COMMAND ----------



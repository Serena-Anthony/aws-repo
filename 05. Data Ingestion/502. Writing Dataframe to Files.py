# Databricks notebook source
# DBTITLE 1,Cleanup
dbutils.fs.rm('/FileStore/write_data', recurse=True)

# COMMAND ----------

# DBTITLE 1,Create Dataframe
order_schema = ['Order_ID', 'Order_Date', 'Order_Quantity', 'Sales', 'Discount', 'Profit', 
                'Unit_Price', 'Customer_Name', 'Product_Category']

order_data = [
    [3,'10/13/2010', 6, 261.54, 0.04, -213.25, 38.94, 'Muhammed MacIntyre', 'Category-1'],
    [6,'10/13/2010', 2, 6.93, 0.01, -4.64, 2.08, 'Ruben Dartt', 'Category-4'],
    [32,'10/13/2010', 15, 140.56, 0.04, -128.38, 8.46, 'Liz Pelletier', 'Category-3'],
    [32,'10/13/2010', 26, 2808.08, 0.07, 1054.82, 107.53, 'Liz Pelletier', 'Category-4'],
    [32,'10/13/2010', 24, 1761.4, 0.09, -1748.56, 70.89, 'Liz Pelletier', 'Category-4'],
    [33,'10/14/2010', 23, 160.2335, 0.04, -85.13, 7.99, 'Liz Pelletier', 'Category-4'],
    [35,'10/14/2010', 30, 288.56, 0.03, 60.72, 9.11, 'Julie Creighton', 'Category-1'],
    [35,'10/14/2010', 14, 1892.848, 0.01, 48.99, 155.99, 'Julie Creighton', 'Category-1'],
    [36,'10/14/2010', 46, 2484.7455, 0.1, 657.48, 65.99, 'Sample Company A', 'Category-4'],
    [65,'10/14/2010', 32, 3812.73, 0.02, 1470.3, 115.79, 'Tamara Dahlen', 'Category-1'],
    [66,'10/15/2010', 41, 108.15, 0.09, 7.57, 2.88, 'Arthur Gainer', 'Category-4'],
    [69,'10/15/2010', 42, 1186.06, 0.09, 511.69, 0.93, 'Jonathan Doherty', 'Category-3'],
    [69,'10/15/2010', 28, 51.53, 0.03, 0.35, 1.68, 'Jonathan Doherty', 'Category-3'],
    [70,'10/15/2010', 48, 90.05, 0.03, -107.0, 1.86, 'Helen Wasserman', 'Category-2'],
    [70,'10/15/2010', 46, 7804.53, 0.05, 2057.17, 205.99, 'Helen Wasserman', 'Category-2'],
    [96,'10/16/2010', 37, 4158.1235, 0.01, 1228.89, 125.99, 'Keith Dawkins', 'Category-3'],
    [97,'10/16/2010', 26, 75.57, 0.03, 28.24, 2.89, 'Craig Yedwab', 'Category-4'],
    [129,'10/16/2010', 4, 32.72, 0.09, -22.59, 6.48, 'Pauline Chand', 'Category-1'],
    [130,'10/16/2010', 3, 461.89, 0.05, -309.82, 150.98, 'Roy Collins', 'Category-3'],
    [130,'10/16/2010', 29, 575.11, 0.02, 71.75, 18.97, 'Roy Collins', 'Category-3']
]


df = spark.createDataFrame(data=order_data, schema=order_schema)
df.show(5)

# COMMAND ----------

# DBTITLE 1,Check the number of partitions of DF


# COMMAND ----------

# DBTITLE 1,Writing CSV file with Default Settings
df.write.csv('/Volumes/serio_cat/default/write_vol/orders.csv')

# COMMAND ----------

# DBTITLE 1,Writing parquet file with Default Settings
df.write.parquet('/Volumes/serio_cat/default/write_vol/orders.parquet')

# COMMAND ----------

# DBTITLE 1,Writing JSON file with Default Settings
df.write.json('/Volumes/serio_cat/default/write_vol/orders.json')

# COMMAND ----------

# DBTITLE 1,Writing CSV: Alternate way
df.write.format('csv').save('/Volumes/serio_cat/default/write_vol/orders.csv', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Writing parquet: Alternate way
df.write.format('parquet').save('/Volumes/serio_cat/default/write_vol/orders.parquet', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Writing JSON: Alternate way
df.write.format('json').save('/Volumes/serio_cat/default/write_vol/orders.json', mode='overwrite')

# COMMAND ----------

# DBTITLE 1,Check files in the directory
# list the files in directory, (while writing CSV file it has written it into multiple partitions)
dbutils.fs.ls('/Volumes/serio_cat/default/write_vol')


for file in dbutils.fs.ls('/Volumes/serio_cat/default/write_vol/orders.csv'):
    if file.name.endswith('.csv'):
        print(file.name)

# COMMAND ----------

# DBTITLE 1,Reduce DF to 1 partition and then write
# to write the file in single part
# header =True to make first entry as column headers

# check number of files in the resultant directory (it should be only one)

df.coalesce(1).write.csv('/Volumes/serio_cat/default/write_vol/orders.csv', mode='overwrite', header=True)

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Compress and then write
df.coalesce(1).write.csv('/Volumes/serio_cat/default/write_vol/orders.csv', mode='overwrite', compression='snappy',header=True)

# COMMAND ----------

# DBTITLE 1,Verify Data
orders_df = spark.read.csv('/Volumes/serio_cat/default/write_vol/orders.csv',header=True)
orders_df.show(5)


# COMMAND ----------

# DBTITLE 1,Writing Pipe Separated CSV file
df.coalesce(1).write.csv('/Volumes/serio_cat/default/write_vol/orders_pipe_sep.csv', mode='overwrite', header=True, sep='|')

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Try reading: without specifying separator
orders_df = spark.read.csv('/Volumes/serio_cat/default/write_vol/orders_pipe_sep.csv', header=True)
orders_df.show(5)

# COMMAND ----------

# DBTITLE 1,Try reading: by specifying separator
orders_df = ?
orders_df.show(5)

# COMMAND ----------

# DBTITLE 1,Writing with option
(df.coalesce(1)
 .write.mode('overwrite')
 .option('compression', 'gzip')
 .option('header', 'true')
 .option('sep', '|')
 .csv('/Volumes/serio_cat/default/write_vol/orders_pipe_sep.csv'))

df.show()


# COMMAND ----------

# DBTITLE 1,Verify Data

?

# COMMAND ----------

# DBTITLE 1,Writing with options
?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Overwrite Mode

# COMMAND ----------

# DBTITLE 1,Initial Write operation
df.coalesce(1).write.mode('overwrite').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_overwrite.csv')

# COMMAND ----------

# DBTITLE 1,confirm no. of records in the file
spark.read.csv('/Volumes/serio_cat/default/write_vol/write_date/orders_overwrite.csv').count()

# COMMAND ----------

# DBTITLE 1,Try writing again
df.coalesce(1).write.mode('overwrite').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_overwrite.csv')


# COMMAND ----------

# DBTITLE 1,confirm no. of records in the file again
spark.read.csv('/Volumes/serio_cat/default/write_vol/write_date/orders_overwrite.csv').count()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Append Mode

# COMMAND ----------

# DBTITLE 1,Initial Write
df.coalesce(1).write.mode('append').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_append.csv')

# COMMAND ----------

# DBTITLE 1,confirm no. of records in the file
spark.read.csv('/Volumes/serio_cat/default/write_vol/write_date/orders_append.csv').count()

# COMMAND ----------

# DBTITLE 1,Try writing again
df.coalesce(1).write.mode('append').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_append.csv')

# COMMAND ----------

# DBTITLE 1,confirm no. of records in the file again
spark.read.csv('/Volumes/serio_cat/default/write_vol/write_date/orders_append.csv').count()

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Error mode
# MAGIC
# MAGIC This is default mode

# COMMAND ----------

# DBTITLE 1,Initial Write
df.coalesce(1).write.csv('/Volumes/serio_cat/default/write_vol/write_date/orders_error.csv')

#mode = error -> if mode isnt specified its an error 
# df.coalesce(1).write.mode('error').csv('/FileStore/write_data/orders_error.csv')

# COMMAND ----------

 df.coalesce(1).write.mode('overwrite').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_error.csv')

# COMMAND ----------

# DBTITLE 1,Try writing again
df.?

# df.coalesce(1).write.mode('error').csv('/FileStore/write_data/orders_error.csv')

# COMMAND ----------

# MAGIC %md
# MAGIC ###Ignore Mode
# MAGIC
# MAGIC ignores all errors - skips the action
# MAGIC ex v try rewritting without rewrite mode - no error - doesnt do the action

# COMMAND ----------

# DBTITLE 1,See Data
df.show(5)

# COMMAND ----------

# DBTITLE 1,Initial write
df.coalesce(1).write.mode('ignore').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_ignore.csv', header=True)

# COMMAND ----------

# DBTITLE 1,verify data in file
df.show(5)

# COMMAND ----------

# DBTITLE 1,Some modifications in DF
from pyspark.sql.functions import col
df = df.withColumn('Order_Quantity', col('Order_Quantity')*100)
df.show(5)

# COMMAND ----------

# DBTITLE 1,Write updated dataframe
df.coalesce(1).write.mode('ignore').csv('/Volumes/serio_cat/default/write_vol/write_date/orders_ignore.csv')

# COMMAND ----------

# DBTITLE 1,How many records in the file?
spark.

# COMMAND ----------

# DBTITLE 1,What's a data in file?
spark.read.

# COMMAND ----------

# DBTITLE 1,Cleanup
dbutils.fs.rm('/Volumes/serio_cat/default/write_vol', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ================================================ END ==============================================

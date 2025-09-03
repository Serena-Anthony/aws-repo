# Databricks notebook source
# DBTITLE 1,Cleanup
dbutils.fs.rm('dbfs:/FileStore/orders_partitioned_by_date', recurse=True)
dbutils.fs.rm('dbfs:/FileStore/orders_partitioned_by_month', recurse=True)
dbutils.fs.rm('dbfs:/FileStore/orders_partitioned', recurse=True)

# COMMAND ----------

# DBTITLE 1,Read JSON file into DataFrame
df = spark.read.json('/Volumes/serio_cat/default/order_vol/orders_data.json')

# COMMAND ----------

# DBTITLE 1,What are the columns and their datatypes?
df.printSchema()

# COMMAND ----------

# DBTITLE 1,How data looks like?
?

# COMMAND ----------

# DBTITLE 1,How many records in Dataframe?
df.count()

# COMMAND ----------

# DBTITLE 1,partitionBy Documentation
help(df.write.partitionBy)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Partitioning by single column

# COMMAND ----------

# DBTITLE 1,Preparing column for splitting data
from pyspark.sql.functions import col, date_format

df = (df.withColumn('order_date', col('order_date').cast('timestamp'))
      .withColumn('order_dt', date_format('order_date', 'yyyyMMdd'))
      .withColumn('order_month', date_format('order_date', 'yyyyMM'))
      .coalesce(1)
)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,see updated dataframe
df.show(5)

# COMMAND ----------

# DBTITLE 1,Write Dataframe into parquet file by partitioning
# partitioning data by date

df.write.partitionBy('order_dt').parquet('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_date')

# COMMAND ----------

# DBTITLE 1,Get records count from written parquet file
# verify the partitioned data

spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_date').count()

# COMMAND ----------

# DBTITLE 1,Partitioning by month
# partition data by order month

df.coalesce(1).write.partitionBy('order_month').parquet('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_month')

# COMMAND ----------

# DBTITLE 1,how many records?
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_month').count()

# COMMAND ----------

# DBTITLE 1,Remove written directories
dbutils.fs.rm('dbfs:/FileStore/orders_partitioned_by_date', recurse=True)
dbutils.fs.rm('dbfs:/FileStore/orders_partitioned_by_month', recurse=True)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Partitioning data by multiple columns
# MAGIC
# MAGIC

# COMMAND ----------

# DBTITLE 1,Prepare data by adding required columns
df = (df.withColumn('year', date_format('order_date', 'yyyy'))
        .withColumn('month', date_format('order_date', 'MM'))
        .withColumn('day', date_format('order_date', 'dd'))
)


# COMMAND ----------

# DBTITLE 1,See Data
df= df.drop('day_of_month')
df.show(5)

# COMMAND ----------

# DBTITLE 1,Partition data by year, month and day_of_month
(df.coalesce(1)
 .write
 .partitionBy('year','month','day')
 .parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned'))

# COMMAND ----------

# DBTITLE 1,Get count from written files
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2013
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned').filter('year=2013').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2014
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned').filter('year=2014').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2013: Alternate
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned/year=2013').count()

# COMMAND ----------

# DBTITLE 1,How many records in 2014: Alternate
spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders_all_partitioned/year=2014').count()

# COMMAND ----------

# DBTITLE 1,Create View
df.createOrReplaceTempView('orders_data')

# COMMAND ----------

# DBTITLE 1,Verify view creation
spark.sql('SHOW TABLES').show()

# COMMAND ----------

# DBTITLE 1,Spark-SQL: Get count from view for 2013 records
spark.sql(
    '''
    select count(*)
    from orders_data
    where year = 2013    
    '''
).show()

# COMMAND ----------

# DBTITLE 1,SQL Syntax
# MAGIC %sql
# MAGIC select count(*) from orders_data where year= 2013;

# COMMAND ----------

# DBTITLE 1,Remove Created Directories
dbutils.fs.rm('/Volumes/serio_cat/default/order_vol/orders_all_partitioned/', recurse=True)
dbutils.fs.rm('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_date/', recurse=True)
dbutils.fs.rm('/Volumes/serio_cat/default/order_vol/orders_partitioned_by_month/', recurse=True)


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ============================================= END ==========================================

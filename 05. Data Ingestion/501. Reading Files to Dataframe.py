# Databricks notebook source
# MAGIC %md
# MAGIC ### Reading CSV File

# COMMAND ----------

# DBTITLE 1,Simplest Example
df = spark.read.csv('/Volumes/serio_cat/default/order_vol/orders.csv')
df.show(5)

# COMMAND ----------

# DBTITLE 1,Schema investigation
df.printSchema()

# COMMAND ----------

# DBTITLE 1,spark.read.csv documentation
help(?)

# COMMAND ----------

# DBTITLE 1,Alternate Syntax
df = spark.read.format('csv').load('/Volumes/serio_cat/default/order_vol/orders.csv')
df.show(2)

# COMMAND ----------

# DBTITLE 1,Specifying header information
df = spark.read.format('csv').load('/Volumes/serio_cat/default/order_vol/orders.csv', header ='True')
df.show(2)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Setting up schema explicitly
order_schema = """
    Order_ID  INT,
    Order_Date DATE,
    Order_Quantity INT,
    Sales FLOAT,
    Discount FLOAT,
    Profit FLOAT,
    Unit_Price FLOAT,
    Customer_Name STRING,
    Product_Category STRING
"""

df = spark.read.format('csv').load('/Volumes/serio_cat/default/order_vol/orders.csv', header ='True', schema = order_schema)
df.show(2)

# date column is not processed correctly

# COMMAND ----------

# DBTITLE 1,See the schema of DataFrame
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Infer schema from data file
df = spark.read.csv('/Volumes/serio_cat/default/order_vol/orders.csv', header=True, inferSchema=True) #identifies schema n gets datatype

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Using option
order_schema = """
    Order_ID  INT,
    Order_Date STRING,
    Order_Quantity INT,
    Sales FLOAT,
    Discount FLOAT,
    Profit FLOAT,
    Unit_Price FLOAT,
    Customer_Name STRING,
    Product_Category STRING
"""

df = (spark
      .read
      .option('header', True)
      .option('schema','order_schema')
      .csv('/Volumes/serio_cat/default/order_vol/orders.csv')
      )
     
df.printSchema()

# COMMAND ----------

df.show(5)

# COMMAND ----------

# DBTITLE 1,Try reading Pipe separated file
df = spark.read.format('csv')
#pipe sep file will give error
df.show(5)

# COMMAND ----------

# DBTITLE 1,could not read it properly
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Specify sep as  pipe(|)
df = spark.read.csv('/Volumes/serio_cat/default/order_vol/orders_pipe_sep.csv', sep='|',header=True, inferSchema=True)
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Specify separator as '|' using option
df = (spark
      .read
      .option('header', True)
      .option('inferSchema', True)
      .option('sep','|')
      .csv('/Volumes/serio_cat/default/order_vol/orders_pipe_sep.csv'))

df.show(5)

# COMMAND ----------

# DBTITLE 1,Now schema looks good
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Using  options
df = ?

df.show(5)

# COMMAND ----------

# DBTITLE 1,Verify correctness of schema
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Know the source file of dataframe
df.?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading JSON file

# COMMAND ----------

# DBTITLE 1,Simplest Example
df = spark.read.json('/Volumes/serio_cat/default/order_vol/orders.json')
df.show(2)

# COMMAND ----------

# DBTITLE 1,Investigate Schema
# Order_Date is taken as string

df.printSchema()

# COMMAND ----------

# DBTITLE 1,spark.read.json Documentation
help(spark.read.json)

# COMMAND ----------

# DBTITLE 1,Specify Schema explicitly while reading
schema = """
    Order_ID  INT,
    Order_Date date,
    Order_Quantity INT,
    Sales FLOAT,
    Discount FLOAT,
    Profit FLOAT,
    Unit_Price FLOAT,
    Customer_Name STRING,
    Product_Category STRING
"""

df = spark.read.schema(schema).json('/Volumes/serio_cat/default/order_vol/orders.json')
df.show(5) 




# COMMAND ----------

df.printSchema()

# COMMAND ----------

# order_Date is taken as date type but could not read values correctly
# date values are not in default format
df = spark.read.json('/Volumes/serio_cat/default/order_vol/orders.json', inferSchema=True)
df.printSchema()

# COMMAND ----------

# DBTITLE 1,Specify Order_Date as String type
schema = """
    Order_ID  INT,
    Order_Date STRING,
    Order_Quantity INT,
    Sales FLOAT,
    Discount FLOAT,
    Profit FLOAT,
    Unit_Price FLOAT,
    Customer_Name STRING,
    Product_Category STRING
"""

df = spark.read.json('/Volumes/serio_cat/default/order_vol/orders.json', schema=schema)
df.show(5) 
df.printSchema()



# COMMAND ----------

df = (
    spark
    .read
    .option('inferSchema', 'True')
    .json('/Volumes/serio_cat/default/order_vol/orders.json')
)

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Explicitly converting Order_Date to Date type
from pyspark.sql.functions import to_date, col

df = df.withColumn('Order_Date', to_date(col('Order_Date'), 'MM/dd/yyyy'))

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.?

# COMMAND ----------

# MAGIC %md
# MAGIC ### Reading parquet file

# COMMAND ----------

# DBTITLE 1,Simplest Example
df = spark.read.parquet('/Volumes/serio_cat/default/order_vol/orders.parquet')
df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

from pyspark.sql.functions import to_date,col

df = df.withColumn('Order_Date', to_date(col('Order_Date'), 'MM/dd/yyyy'))

df.show(5)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

df.?

# COMMAND ----------

help(?)

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Reading Multiple Files

# COMMAND ----------

df = (spark
      .read
      .option('header', True)
      .option('inferSchema', True)
      .csv('/Volumes/serio_cat/default/order_vol/multi_csv/'))
     
df.printSchema()

# COMMAND ----------

df.count()

# COMMAND ----------

df.show()

# COMMAND ----------

df.inputFiles()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ======================================= END ============================================

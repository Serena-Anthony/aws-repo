# Databricks notebook source
import datetime
from pyspark import Row

user_list = [
    {
        'id': 1,
        'f_name': 'John',
        'l_name': 'Maddocks',
        'email': 'abc@xyz.com',
        'phone_numbers': Row(mobile='+1 234 567 8901', home='+1 234 444 5556'),
        'courses': [3],
        'is_customer': True,
        'amount_paid': 1001.22,
        'customer_from': datetime.date(2020, 2, 1),
        'last_updated': datetime.datetime(2021, 3, 4, 10, 40, 25)
    },
    {
        'id': 2,
        'f_name': 'Bob',
        'l_name': 'Miller',
        'email': 'abc@xyz.com',
        'phone_numbers': Row(mobile='+1 234 567 9999', home='+1 234 444 7777'),
        'courses': [2, 4],
        'is_customer': False,
        'amount_paid': 1000.5,
        'customer_from': datetime.date(2021, 6, 1),
        'last_updated': datetime.datetime(2022, 3, 4, 10, 40, 25)
    },
        {
        'id': 3,
        'f_name': 'Chris',
        'l_name': 'Miller',
        'email': 'abc@xyz.com',
        'phone_numbers': Row(mobile='+1 234 444 7777', home=None),
        'courses': [],
        'is_customer': True,
        'amount_paid': 1004.5,
        'customer_from': datetime.date(2021, 6, 1),
        'last_updated': datetime.datetime(2022, 3, 4, 10, 40, 25)
    },
        {
        'id': 4,
        'f_name': 'christy',
        'l_name': 'Marsh',
        'email': 'abc@xyz.com',
        'phone_numbers': Row(mobile=None, home=None),
        'is_customer': True,
        'amount_paid': 1004.5,
        'customer_from': datetime.date(2021, 6, 1),
        'last_updated': datetime.datetime(2022, 3, 4, 10, 40, 25)
    }
]

df = spark.createDataFrame(user_list)
df.show()

# COMMAND ----------

# DBTITLE 1,Concatenating values from multiple columns
from pyspark.sql.functions import col, concat, lit

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Changing Date Format
from pyspark.sql.functions import date_format

temp_df = df.select(?)
temp_df.show()

# COMMAND ----------

# DBTITLE 1,Know about available date_formats
# https://spark.apache.org/docs/3.5.3/sql-ref-datetime-pattern.html

# COMMAND ----------

# DBTITLE 1,Separating Date components
customer_data = df.select(?)
customer_data.show()

# COMMAND ----------

# DBTITLE 1,Manipulation on Numeric column
?

# COMMAND ----------

# DBTITLE 1,withColumn() example
df.select('id', 'f_name', 'l_name').?

# COMMAND ----------

# DBTITLE 1,withColumnRenamed() Example 1
#renaming a single column

df.select('id', 'f_name', 'l_name').?

# COMMAND ----------

# DBTITLE 1,withColumnRenamed() Example 2
# renaming multiple columns

(
    ?
    .show()
)

# COMMAND ----------

# DBTITLE 1,Renaming the columns and expressions using alias
df.select(?).show()

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# DBTITLE 1,Applying size on Arraytype
from pyspark.sql.functions import size

df.select('id', 'courses').?).show()

# COMMAND ----------

# DBTITLE 1,Preparing dummy data to demonstrate functions
employees = [(1, 'Scott', 'Tiger', 1000.0, 'united states', '+1 123 456 7890', '123 45 6789'),
             (2, 'Henry', 'Ford', 1250.0, 'India', '+91 234, 567 8901', '456 78 9123'),
             (3, 'Nick', 'Junior', 750.0, 'united KINGDOM', '+44 111 111 1111', '222 33 4444'),
             (4, 'Bill', 'Gomes', 1500.00, 'AUSTRALIA', '+61 987 654 3210', '789 12 6118')]

schema='''emp_id INT, f_name STRING, l_name STRING, salary FLOAT, nationality STRING, phone_number STRING, ssn STRING'''
emp_df = spark.createDataFrame(data=employees, schema=schema)

emp_df.show()

# COMMAND ----------

# DBTITLE 1,Importing Pyspark Functions
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,20% Hike in Salary: just try
# no error but doesn't work
# understanding role of col

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,20% Hike in Salary: Correct way
# this will work

emp_df.?.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### String Manipulations

# COMMAND ----------

# DBTITLE 1,String Lower Function
# using lower() function
# mentioned column name in withColumn is same as original column name
# it will overright orignal column in the dataframe

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,String Upper Function
# using upper() function
# creating new column

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,Initcap
emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,Length
emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,Substring
emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,Split
emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,What are the components?
# split results in Array type column

emp_df.?.printSchema()

# COMMAND ----------

# DBTITLE 1,TODO: Try other string functions
# Try: trim, ltrim, rtrim, lpad, rpad, instr, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Datetime Manipulation

# COMMAND ----------

# DBTITLE 1,Get started
# creating dummy dataframe
l = [('X', )]
df = spark.createDataFrame(l).toDF('dummy')

# get current date
print(?).show(truncate=False)) # default/standard format #yyyy-MM-dd

# get current date and time
print(?).show(truncate=False)) # default/standard format #yyyy-MM-dd HH:mm:ss.SSS

# COMMAND ----------

# DBTITLE 1,Getting components of the date object
df.select().show()

# COMMAND ----------

# DBTITLE 1,Getting components of the timestamp object
df.select().show(truncate=False)

# COMMAND ----------

# DBTITLE 1,to_date() Example 1
# converting string into date

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,to_date() Example 2
# to_date

df.select(?).show() # julian day / three digit day of the year

# COMMAND ----------

# DBTITLE 1,to_date() Example 3
# to_date

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,to_timestamp() Example 1
df.select(?).show()

# COMMAND ----------

# DBTITLE 1,to_timestamp() Example 2
df.select(?).show()

# COMMAND ----------

# date-time formats

# yyyy: 4 digit year
# MM : 2 digit month
# MMM : 3 letter abbreviated month
# MMMM : full month name
# dd : day of the month
# DD : day of the year/ julian day
# HH : hours (24 hours format)
# hh : hours (12 hours format)
# mm : minutes
# ss : seconds
# SSS : milliseconods
# EE : 3 letter abbreviated weekday

# COMMAND ----------

# DBTITLE 1,Creating sample dataframe for date manipulation
datetimes = [
    ("2021-01-28", '2021-01-28 10:00:00.123'),
    ("2022-02-18", '2022-02-18 08:08:08.993'),
    ("2023-02-25", '2023-02-25 11:59:59.343'),
    ("2023-02-23", '2023-02-23 00:00:00.000'),
]

dt_tm_df = spark.createDataFrame(datetimes, schema='date STRING, time STRING')

dt_tm_df.show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Adding and subtracting dates to/from existing dates and times
dt_tm_df. \
    withColumn('date_add_date', F.date_add('date', 10)). \
    withColumn('date_add_time', F.date_add('time', 10)). \
    withColumn('date_sub_date', F.date_sub('date', 10)). \
    withColumn('date_sub_time', F.date_sub('time', 10)). \
show(truncate=False)

# COMMAND ----------

# DBTITLE 1,datediff(end, start)
# gives diff between 2 given dates in days

dt_tm_df.?.show()

# COMMAND ----------

# DBTITLE 1,TODO: try other date time functions
# Try: months_between, add_months, trunc, date_trunc, etc.

# COMMAND ----------

# MAGIC %md
# MAGIC ### Manipulating with Null values in Spark Dataframes

# COMMAND ----------

# DBTITLE 1,Preparing dummy data to demonstrate Date functions
employees = [(1, 'Scott', 'Tiger', 1000.0, 10, 'united states', '+1 123 456 7890', '123 45 6789'),
             (2, 'Henry', 'Ford', 1250.0, None, 'India', '+91 234, 567 8901', '456 78 9123'),
             (3, 'Nick', 'Junior', 750.0, '', 'united KINGDOM', '+44 111 111 1111', '222 33 4444'),
             (4, 'Bill', 'Gomes', 1500.00, 10, 'AUSTRALIA', '+61 987 654 3210', '789 12 6118')]

schema = '''emp_id INT, f_name STRING, l_name STRING, salary FLOAT, bonus STRING, nationality STRING, phone_number STRING, ssn STRING'''

emp_df = spark.createDataFrame(employees, schema=schema)

emp_df.show()

# COMMAND ----------

# DBTITLE 1,coalesce() Example 1
# creating new column by replacing null with 0 (empty string ('') is not getting replaced)

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,coalesce() Example 2
# changing datatype makes the empty string as null (empty string as no equivalent int so gets converted to null), then apply coalesce

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,Putting all together
# calculate payment

emp_df.?.show()

# COMMAND ----------

# DBTITLE 1,nvl() through expr()
# with expr (sql expression)
# nvl is not direct function in pyspark.Functions

emp_df.?.show()
   
# emp_df.withColumn('bonus', F.nvl(bonus, 0)).show()  -- this gives error

# COMMAND ----------

# complete solution with expr

emp_df.?.show()

# COMMAND ----------

# retrun true if empty of null and false otherwise

emp_df.?.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ======================================== END ===========================================

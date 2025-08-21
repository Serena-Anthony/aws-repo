# Databricks notebook source
# DBTITLE 1,Creating python object
# creating dataframe from list of list

data = [[1,'John'],[2,'Bob'],[3,'Micky'],[4,'Donald'],[5,'Elvis']]
type(data)

# COMMAND ----------

# DBTITLE 1,Creating DataFrame from list of list
## without schema specification

df1 = ?
df1.?

# COMMAND ----------

# with schema specification

df2 = ?
df2.show()

# COMMAND ----------

# DBTITLE 1,Another DataFrame
data = [ ("James", "Sales", 3000),
    ("Michael", "Sales", 4600),
    ("Robert", "Sales", 4100),
    ("Maria", "Finance", 3000),
    ("James", "Sales", 3000),
    ("Scott", "Finance", 3300),
    ("Jen", "Finance", 3900),
    ("Jeff", "Marketing", 3000),
    ("Kumar", "Marketing", 2000),
    ("Saif", "Sales", 4100),
    ("Joe", "Sales", 4200),
    ("Venkat", "Sales", 4000)      
   ]

empDf = ?

# COMMAND ----------

# DBTITLE 1,Get Column Names
empDf.?

# COMMAND ----------

# DBTITLE 1,Get Column Names and Datatypes
empDf.?

# COMMAND ----------

# DBTITLE 1,Display the Cchema of Dataframe Object
empDf.?

# COMMAND ----------

# DBTITLE 1,Retrieve Data from DataFrame
# it returns collection of Row objects

empDf.?

# COMMAND ----------

# DBTITLE 1,Display dataframe object
# commonly used dataframe function

empDf.?

# COMMAND ----------

# DBTITLE 1,Getting specific number of records
empDf.?

# COMMAND ----------

# DBTITLE 1,Interactive Table
# This is Databricks specific function

empDf.?

# COMMAND ----------

# DBTITLE 1,Convert DataFrame object into RDD object
rdd_obj = ?

# COMMAND ----------

# DBTITLE 1,Validate the conversion
type(rdd_obj)

# COMMAND ----------

# DBTITLE 1,How many partitions?
rdd_obj.getNumPartitions()

# COMMAND ----------

# DBTITLE 1,Data in RDD
rdd_obj.collect()

# COMMAND ----------

# DBTITLE 1,Partition-wise Data in RDD
rdd_obj.glom().collect()

# COMMAND ----------

# DBTITLE 1,What happens in background
# spark will not optimize RDD
# optimizer is not used while using RDD
# optimizer is used in DF
# we write a code in DataFrame like empDf.filter(empDf.salary >= 4200), spark will generate a scala code in RDD

# COMMAND ----------

# MAGIC %md
# MAGIC ### Row() Function

# COMMAND ----------

# DBTITLE 1,Importing and Creating Row
from pyspark.sql import Row
row = ?

# COMMAND ----------

# DBTITLE 1,Accessing Fields from Row
row.?

# COMMAND ----------

# DBTITLE 1,Just another way
row[?]

# COMMAND ----------

data_rows = [?]
data_rows

# COMMAND ----------

df2 = spark.createDataFrame(data_rows, schema=['name', 'dept', 'salary'])
df2.show()

# COMMAND ----------

# DBTITLE 1,Setting up Data and Schema
# data with date and time types

import datetime

users_data = [(1, 'Ajit', 'aaa@xyz.com', True, 1002.55, datetime.date(2021, 2, 10), datetime.datetime(2021, 4, 11, 12, 30,  40)),
        (2, 'Brijesh', 'bbb@xyz.com', False, 11122.55, datetime.date(2021, 3, 11), datetime.datetime(2021, 5, 16, 12, 30,  40))]

user_schema = '''
id INT,
f_name STRING,
email STRING,
is_customer BOOLEAN,
amount_paid FLOAT,
customer_from DATE,
last_updated_ts TIMESTAMP
'''

# COMMAND ----------

# DBTITLE 1,# creating Dataframe with String Schema
df2 = spark.createDataFrame(?)
df2.show()

# COMMAND ----------

from pyspark.sql.types import *


fields =StructType([
    StructField('id', IntegerType()),
     StructField('f_name', StringType()),
     StructField('email', StringType()),
     StructField('is_customer', BooleanType()),
     StructField('amount_paid', FloatType()),
     StructField('customer_from', DateType()),
     StructField('last_updated_ts', TimestampType()),
])

type(?)

# COMMAND ----------

# DBTITLE 1,Creating Dataframe Using StructType
df3 = spark.createDataFrame(?)
df3.show()

# COMMAND ----------

# DBTITLE 1,Sample Dataset
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

# COMMAND ----------

import pandas as pd
pandas_df = ?

# COMMAND ----------

spark_df = ?
spark_df.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ===================================== END ===================================

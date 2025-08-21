# Databricks notebook source
# MAGIC %md
# MAGIC ### Array Type

# COMMAND ----------

# DBTITLE 1,Example Dataset
# pay attention to 'phone_numbers' field

import datetime

user_list = [
    {
        'id': 1,
        'f_name': 'John',
        'l_name': 'Maddocks',
        'email': 'abc@xyz.com',
        'phone_numbers': ['+1 234 567 8901', '+1 234 444 5556'],
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
        'phone_numbers': ['+1 234 567 9999', '+1 234 444 7777'],
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
        'phone_numbers': ['+1 234 444 7777'],
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
        'phone_numbers': None,
        'is_customer': True,
        'amount_paid': 1004.5,
        'customer_from': datetime.date(2021, 6, 1),
        'last_updated': datetime.datetime(2022, 3, 4, 10, 40, 25)
    }
]

# COMMAND ----------

# DBTITLE 1,Creating Dataframe
df = spark.?
df.show()

# COMMAND ----------

# DBTITLE 1,What type of data in columns?
# phone number is array type

df.dtypes

# COMMAND ----------

# DBTITLE 1,See Specific Columns of Dataframe
## setting truncate=False to see complet values in a the column
## select function helps to include specific columns from given dataframe (* to include all the columns)

df.select('id', 'phone_numbers').show(?)

# COMMAND ----------

# DBTITLE 1,Select Example 1
df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Select Example 2
df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Select Example 3
df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Select Example 4
cols = ['id', 'f_name', 'l_name']
df.select(?).show()

# COMMAND ----------

help(df.select)

# COMMAND ----------

# DBTITLE 1,col() Function
from pyspark.sql.functions import col

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,What is col() function?
#col function returns object of Column  type

# If there are no transformations on any column in any function then we should be able to pass all column names as strings.
# If we want to apply transformations using some of the functions then passing column names as strings will not suffice. We have to pass them as column type.

obj = col('f_name')

type(obj)

# COMMAND ----------

# DBTITLE 1,ERROR
# try below statement, it will give an error

# df.select('id', 'phone_numbers'[0], 'phone_numbers'[1]).show()

# COMMAND ----------

# DBTITLE 1,Explode Function
from pyspark.sql.functions import ?

## will not make changes in-place
## withColumn is used to apply any transformations on existing Dataframe column and create new column out of it
## syntax: withColumn(column_name, any_operation_on_column)

df.?

# COMMAND ----------

# DBTITLE 1,explode_outer function
# Explode function excluding the rows where value in null in spcified column (id 4 record in above example)
# explode_outer is alternate to explode() if all records need to be considered

from pyspark.sql.functions import ?

df.?

# COMMAND ----------

# MAGIC %md 
# MAGIC ### Map Type

# COMMAND ----------

# DBTITLE 1,Example Dataset
# pay attention to 'phone_numbers' field

import datetime
user_list = [
    {
        'id': 1,
        'f_name': 'John',
        'l_name': 'Maddocks',
        'email': 'abc@xyz.com',
        'phone_numbers': {'mobile':'+1 234 567 8901', 'home':'+1 234 444 5556'},
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
        'phone_numbers': {'mobile':'+1 234 567 9999', 'home':'+1 234 444 7777'},
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
        'phone_numbers': {'mobile':'+1 234 444 7777'},
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
        'phone_numbers': None,
        'is_customer': True,
        'amount_paid': 1004.5,
        'customer_from': datetime.date(2021, 6, 1),
        'last_updated': datetime.datetime(2022, 3, 4, 10, 40, 25)
    }
]

# COMMAND ----------

# DBTITLE 1,Create Dataframe
df = spark.createDataFrame(user_list)
df.show()

# COMMAND ----------

# DBTITLE 1,Datatypes of columns
df.dtypes

# COMMAND ----------

# DBTITLE 1,See data in phone_numbers
df.select('id', 'phone_numbers').show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Accessing only mobile numbers
from pyspark.sql.functions import col

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Accessing only phone numbers
from pyspark.sql.functions import col

df.select(?).show()

# COMMAND ----------

# DBTITLE 1,Accessing both numbers and giving Alias
df.select('id', ?).show()

# COMMAND ----------

# DBTITLE 1,What is alias
# it is used to give alternate name to table or column

df.?

# COMMAND ----------

# DBTITLE 1,explode on map type
# it create two columns from 'phone_numbers'--> key and value

df.?

# COMMAND ----------

# DBTITLE 1,Renaming key and value columns
# withColumnRenamed is used to rename existing column of dataframe
# it doesn't rename the column inplace, it creates another dataframe
# syntax : withColumnRenamed(old_Col_Name, new_Col_Name)

df.?

# COMMAND ----------

# DBTITLE 1,Selecting all the columns
df.?

# COMMAND ----------

# DBTITLE 1,Applying explode_outer()
df.?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Struct Type

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

# DBTITLE 1,Creating DataFrame
df = spark.createDataFrame(user_list)

# COMMAND ----------

# DBTITLE 1,See the column Datatypes
# phone_numbers is struct type

df.dtypes

# COMMAND ----------

# DBTITLE 1,See phone_numbers data
df.select('id', 'phone_numbers').show(truncate=False)

# COMMAND ----------

# DBTITLE 1,Accessing fields from struct
df.select('id', 'phone_numbers.mobile', 'phone_numbers.home').show()

# COMMAND ----------

# DBTITLE 1,Just Another way
from pyspark.sql.functions import col

df.select('id', col('phone_numbers')['mobile'], col('phone_numbers')['home']).show()

# COMMAND ----------

# DBTITLE 1,One more way
df.select('id', col('phone_numbers.*')).show()

# COMMAND ----------

# MAGIC %md
# MAGIC ###Summary
# MAGIC
# MAGIC - Advanced types in Pyspark
# MAGIC   - __Array__ - equivalent to python list
# MAGIC   - __Map__ - equivalent to python dictionary
# MAGIC   - __Struct__ - suitable when structure of all the records is same

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ==================================== END =====================================

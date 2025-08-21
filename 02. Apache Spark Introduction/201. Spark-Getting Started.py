# Databricks notebook source
# DBTITLE 1,Pre-created Spark Context
?

# COMMAND ----------

# DBTITLE 1,Pre-created Spark Session
?

# COMMAND ----------

# DBTITLE 1,Creating RDD
rdd = sc.?

# COMMAND ----------

# DBTITLE 1,Check the type of object
type(rdd)

# COMMAND ----------

# DBTITLE 1,What is inside?
rdd.collect()

# COMMAND ----------

# DBTITLE 1,Basic functions on RDD
print(?)
print(?)
print(?)
print(?)
print(?)

# COMMAND ----------

# DBTITLE 1,How many partitions?
rdd.?

# COMMAND ----------

# DBTITLE 1,Function to check if the number is odd or not
def odd(n):
    print(n)
    print("odd called")
    return n%2 == 1

# COMMAND ----------

# DBTITLE 1,Filter the items from rdd using above function
oddRdd = rdd.?
oddRdd.?

# COMMAND ----------

# DBTITLE 1,RDD is immutable: unchanged in above operation
rdd.?

# COMMAND ----------

# DBTITLE 1,What is oddRdd?
oddRdd.?

# COMMAND ----------

# DBTITLE 1,Creating evenRdd with lambda function
evenRdd = rdd.filter(?)

# COMMAND ----------

# DBTITLE 1,What is inside evenRdd?
evenRdd.?

# COMMAND ----------

# DBTITLE 1,Further operation on evenRdd
evenRddMulBy10Rdd = evenRdd.map(?)

# COMMAND ----------

# DBTITLE 1,What is a result?
evenRddMulBy10Rdd.?

# COMMAND ----------

# DBTITLE 1,How rdd looks like?
rdd.?

# COMMAND ----------

# DBTITLE 1,Getting the details of partitions
rdd.?

# COMMAND ----------

# DBTITLE 1,Specify number of partitions while creating RDD
numRdd = sc.parallelize(?)            # 2 is num of partitions

# COMMAND ----------

# DBTITLE 1,Documentation of parallelize function
help(sc.parallelize)

# COMMAND ----------

# DBTITLE 1,How many partitions this time?
numRdd.?

# COMMAND ----------

# DBTITLE 1,What is inside every partition?
numRdd.?

# COMMAND ----------

# DBTITLE 1,What if too many partitions?
numRdd = sc.parallelize(?)                     # 10 is num of partitions

# COMMAND ----------

# DBTITLE 1,How many partitions?
numRdd.?

# COMMAND ----------

# DBTITLE 1,What are the partitions?
numRdd.?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ##Role of Partitions in Spark 
# MAGIC
# MAGIC **1 partition = 1 task**
# MAGIC
# MAGIC ### How to increase/decrease the partitions at runtime
# MAGIC

# COMMAND ----------

# DBTITLE 1,All numbers in one partitions
rdd = sc.parallelize(?)

# all numbers in 1 partition
# 8 parallel tasks at a time
# only one task = due to 1 partitions
# task completion takes time
# poor resource planning, other parallel threads do not have work

print("Rdd: ", ?)

# COMMAND ----------

# DBTITLE 1,Increase partitions: repartition()
# increase the partitions to 8

rdd2 = rdd.?

print(rdd2.?)

# COMMAND ----------

# DBTITLE 1,Too many partitions
rdd = sc.parallelize(?)
rdd.?

# COMMAND ----------

# DBTITLE 1,Decreasing partitions: repartition()
rdd2 = rdd.?
rdd2.?

# COMMAND ----------

# DBTITLE 1,Decreasing partitions: coalesce()
rdd2 = rdd.?
rdd2.?

# COMMAND ----------

# DBTITLE 1,Increasing partitions: coalesce()
rdd3 = rdd2.?
rdd3.?

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Repartition vs. Coalesce
# MAGIC
# MAGIC 1. Repartition results into full shuffle and coalesce does not
# MAGIC 2. Coalecse is efficient as compaired to repartition
# MAGIC 3. repartition guarentees balanced partitions and coalesce might generrate imbalanced partitions
# MAGIC 4. Repartition is suitable for increasing as well as descresing the numbe rof partitions
# MAGIC 5. Use repartition when changing the number of partitions or evenly distributing data across partitions
# MAGIC 6. Use coalesce when decreasing the number of partitions without incurring the cost of a full shuffle

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ======================================== END ==========================================

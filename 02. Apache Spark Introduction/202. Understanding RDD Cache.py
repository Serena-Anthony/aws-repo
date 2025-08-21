# Databricks notebook source
# DBTITLE 1,Create RDD
rdd = ?

# COMMAND ----------

# MAGIC %md
# MAGIC Now we will build RDD pipeline

# COMMAND ----------

# DBTITLE 1,Filtering RDD
def odd(n):
    print('odd called', n)
    return n%2 == 1

rdd2 = rdd.?

# COMMAND ----------

# DBTITLE 1,Mapping a operation to RDD
def mul(n):
    print('mul called', n)
    return n*10

rdd3 = rdd2.?

# COMMAND ----------

# DBTITLE 1,Create Actions
## Without cache
# it will create 3 independent jobs

print('count', ?)
print('sum', ?)
print('min', ?)

# COMMAND ----------

# DBTITLE 1,RDD is not cached by default
print('is cached', ?)

# COMMAND ----------

# DBTITLE 1,Cache RDD
## Cache the results
rdd3.?

# COMMAND ----------

# DBTITLE 1,Verify RDD Cache
print('is cached', rdd3.?)

# COMMAND ----------

# DBTITLE 1,Separator for output
sc.parallelize([1]).map(lambda n: print ("--------")).count()


# COMMAND ----------

# DBTITLE 1,Again Create some Actions
print('count', ?)
print('sum', ?)
print('min', ?)

# COMMAND ----------

# DBTITLE 1,Caching with persist()
rdd2.?
rdd2.?

# COMMAND ----------

# DBTITLE 1,removing cache with unpersist()
rdd2.?
rdd2.?

# COMMAND ----------

# MAGIC %md
# MAGIC ### cache() Vs. persist()
# MAGIC - RDD.cache() caches the RDD with the default storage level MEMORY_ONLY
# MAGIC - DataFrame.cache() caches the DataFrame with the default storage level MEMORY_AND_DISK
# MAGIC - The persist() method is used to store it to the user-defined storage level

# COMMAND ----------

# DBTITLE 1,Know Cache storage level
rdd3.?
print(rdd3.?)

# COMMAND ----------

# DBTITLE 1,Setting up Storage Level with Persist
from pyspark import StorageLevel
rdd2.?
print(rdd2.?)

# COMMAND ----------

# DBTITLE 1,Changing Storage Level
rdd2.?
rdd2.?
print(rdd2.?)

# COMMAND ----------

# DBTITLE 1,Know more about Cache
# managing cache store in disk and/or memory with or without replication

# documentation links: 
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.cache.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.RDD.persist.html
# https://spark.apache.org/docs/latest/api/python/reference/api/pyspark.StorageLevel.html


# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ========================================= END ==============================================

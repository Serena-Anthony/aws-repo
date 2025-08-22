# Databricks notebook source
# DBTITLE 1,Create Dataframe
import datetime

emp_schema = ['empno', "ename", "job", "mgr", "hiredate", "sal", "comm", "deptno"]

emp_data = [ 
            (7839, 'KING', 'PRESIDENT', None, datetime.date(1981, 1, 17), 5000, None, 10),
            (7698, 'BLAKE', 'MANAGER', 7839, datetime.date(1981, 5, 1), 2850, None, 30),
            (7782, 'CLARK', 'MANAGER', 7839, datetime.date(1981, 6, 9), 2450, None, 10),
            (7566, 'JONES', 'MANAGER', 7839, datetime.date(1981, 4, 2), 2975, None, 20),
            (7788, 'SCOTT', 'ANALYST', 7566, datetime.date(1981, 12, 9), 3000, None, 20),
            (7902, 'FORD', 'ANALYST', 7566, datetime.date(1981, 12, 3), 3000, None, 20),
            (7369, 'SMITH', 'CLERK', 7902, datetime.date(1988, 12, 17), 800, None, 20),
            (7499, 'ALLEN', 'SALESMAN', 7698, datetime.date(1981, 2, 20), 1600, 300, 30),
            (7521, 'WARD', 'SALESMAN', 7698, datetime.date(1981, 2, 22), 1250, 500, 30),
            (7654, 'MARTIN', 'SALESMAN', 7698, datetime.date(1981, 9, 28), 1250, 1400, 30),
            (7844, 'TURNER', 'SALESMAN', 7698, datetime.date(1981, 9, 8), 1500, 0, 30),
            (7876, 'ADAMS', 'CLERK', 7788, datetime.date(1983, 1, 12), 1100, None, 20),
            (7900, 'JAMES', 'CLERK', 7698, datetime.date(1981, 12, 3), 950, None, 30),
            (7934, 'MILLER', 'CLERK', 7782, datetime.date(1982, 1, 23), 1300, None, 10)
           ]

emp_df = spark.createDataFrame(data=emp_data, schema=emp_schema)
emp_df.show()

# COMMAND ----------

from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Total aggregation
emp_df.select(F.count('*')).show()   

# COMMAND ----------

# DBTITLE 1,Grouped aggregation or by-key aggregation
emp_df.filter('sal> 1000').groupBy('deptno').agg(F.count('*')).show()  
#filter v cn use b4 group by like where in sql

# COMMAND ----------

# DBTITLE 1,Filter, select, aggregation, alias
# get salary summation for a given deptno

emp_df.filter('deptno = 10').select(F.sum('sal').alias('tot_sal_dep_10')).show()

# COMMAND ----------

# DBTITLE 1,Multiple aggregation functions
# get count of employees, total_sal, min_sal, max_sal avg_sal for dept 10
?

# COMMAND ----------

# DBTITLE 1,groupBy and agg
# get department wise summary
emp_df.groupBy('deptno').agg(F.count('*')).show()

# COMMAND ----------

# MAGIC
# MAGIC %md
# MAGIC ===================================== END ========================================

# COMMAND ----------

# avg sal for managers ?
# how many ppl joined aftr 31st dec 1981 
# get the count of ppl & total sal paid, deptwise-jobwise

# COMMAND ----------

emp_df.where('job = "MANAGER"').select(F.sum('sal')).show()

# COMMAND ----------

emp_df.where(F.col('hiredate') > datetime.date(1981, 12, 31)).select(F.count('*')).show()

# COMMAND ----------

emp_df.groupBy('deptno', 'job').agg(F.count('*'), F.sum('sal')).show()

# COMMAND ----------



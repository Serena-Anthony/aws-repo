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

# DBTITLE 1,What is filter function?
help(emp_df.filter)

# COMMAND ----------

# DBTITLE 1,What is where() function
help(emp_df.where)

# it is an alias for filter function

# COMMAND ----------

# DBTITLE 1,import functions module for subsequent usage
from pyspark.sql import functions as F

# COMMAND ----------

# DBTITLE 1,Filter: using col function
emp_df.filter(F.col('empno') == 7839).show()

# COMMAND ----------

# DBTITLE 1,Filter: using column name reference
emp_df.where(F.col('empno') == 7839).show()

# COMMAND ----------

# DBTITLE 1,Where: using col function
emp_df.where(emp_df['empno'] == 7839).show()

# COMMAND ----------

# DBTITLE 1,Where: using column name reference
emp_df.where(F.col('empno') == 7839).show()

# COMMAND ----------

# DBTITLE 1,Filter: Condition as an argument in form of string
emp_df.filter('empno= 7839').show()

# COMMAND ----------

# DBTITLE 1,Where: Condition as an argument in form of string
emp_df.where('empno= 7839').show()

# COMMAND ----------

# DBTITLE 1,Creating view
emp_df.createOrReplaceTempView('emp')

# COMMAND ----------

# DBTITLE 1,Spark SQL
# equivalent spark sql statement on the view for above operations
spark.sql(
  '''
  select * 
  from emp
  where empno = 7839
  '''
).show()

# COMMAND ----------

# DBTITLE 1,SQL Query
# MAGIC %sql
# MAGIC
# MAGIC select * from emp where empno= 7839;

# COMMAND ----------

# MAGIC %md 
# MAGIC
# MAGIC ### conditions and operators related to spark dataframes
# MAGIC

# COMMAND ----------

# DBTITLE 1,Condition on string column
## string values are CASE sensitive

emp_df.filter(F.col('ename') == 'ALLEN').show()

# COMMAND ----------

# DBTITLE 1,Condition on numeric column
emp_df.filter(F.col('sal') > '2000').show()
# Try this : it also works
emp_df.filter(F.col('sal') > 2000).show()

# COMMAND ----------

# DBTITLE 1,Non equality
emp_df.filter(F.col('sal') != '2000').show()

# COMMAND ----------

# DBTITLE 1,Multiple conditions
emp_df.filter((F.col('deptno') == 10 )| (F.col('job') == 'MANAGER')).show()

# COMMAND ----------

emp_df.filter((F.col('deptno') == 10 ) & (F.col('job') == 'MANAGER')).show()

# COMMAND ----------

# DBTITLE 1,SQL syntax for multiple conditions
emp_df.filter("deptno == 10  or job == 'MANAGER'").show()

# COMMAND ----------

# DBTITLE 1,Condition with null
emp_df.filter(F.col('mgr').isNull()).show()

# COMMAND ----------

# DBTITLE 1,Condition with not null
emp_df.filter(F.col('comm').isNotNull()).show()

# COMMAND ----------

# DBTITLE 1,Selecting specific columns
emp_df.select('empno','ename','sal').show()

# COMMAND ----------

# DBTITLE 1,Using select and filter together
# between for range based filter

emp_df.select('empno','ename','sal').filter(F.col('sal').between(1500,2500)).show()

# COMMAND ----------

emp_df.createOrReplaceTempView('emp')


# COMMAND ----------

# MAGIC %sql
# MAGIC select deptno, min(sal) as lowest_sal from emp where job= 'MANAGER'
# MAGIC group by deptno order by lowest_sal;

# COMMAND ----------

# DBTITLE 1,Just another syntax
emp_df.select('empno','ename','sal').filter('sal between 1500 and 2500').show()

# COMMAND ----------

emp_df.filter('sal between 1500 and 2500').select('empno','ename','sal').show()

# COMMAND ----------

# DBTITLE 1,isin()
# Python way of doing it

emp_df.select('ename','deptno').filter(F.col('deptno').isin(10,20)).show()

# COMMAND ----------

# DBTITLE 1,in()
# sql way of doing it

emp_df.select('ename','deptno').filter('deptno in (10,20)').show()

# COMMAND ----------

# DBTITLE 1,and conditions
# whose city is empty string or null

emp_df.?.show()

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ====================================== END ======================================

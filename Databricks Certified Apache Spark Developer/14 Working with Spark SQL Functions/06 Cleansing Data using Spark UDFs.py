# Databricks notebook source
import pandas as pd

# COMMAND ----------

courses = {'course_id': ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10'],
           'course_name': ['Mastering SQL', 'Streaming Pipelines - Python', 'Head First Python',
                           'Designing Data-Intensive Applications', 'Distributed Systems', 'Database Internals',
                           'Art of Immutable Architecture', 'Graph Databases', 'Building MicroServices',
                           'Kubernetes Patterns'],
           'course_author': ['Mike Jack', 'Bob Davis', 'Elvis Presley', 'Martin Kleppmann', 'Sukumar Ghosh',
                             'Alex Petrov',
                             'Michael L. Perry', 'Ian Robinson', 'Sam Newman', 'Rolan Hub'],
           'course_status': ['   published   ', '   inactive   ', '\\N', 'published  ', '\\N', '   inactive',
                             'published   ', '\\N', '  inactive ', 'published   '],
           'course_published_dt': ['2020-07-08', '2020-03-10', '\\N', '2021-02-27', '\\N', '2021-05-14',
                                   '2021-04-18', '\\N',
                                   '2020-12-15', '2021-07-11']}

courses_df = spark.createDataFrame(pd.DataFrame(courses))

# COMMAND ----------

users = {'user_id': ['1001', '1002', '1003', '1004', '1005', '1006'],
         'user_name': ['BenJohnson   ', '  Halley Battles ', '  Laura Anderson  ', '  Rolanda Garza ',
                       'Angela Fox  ', 'Kerl Goldinger '],
         'user_email': ['benjohn@gmail.com', '\\N', '\\N', 'garza.roland@gmail.com', 'nshaiary@aol.com',
                        'k.gold@live.com1'],
         'user_gender': ['Male', 'Male', 'Female', 'Male', 'Female', 'Male']}

users_df = spark.createDataFrame(pd.DataFrame(users))

# COMMAND ----------

course_enrolments = {'course_id': ['3', '5', '8', '5', '6', '8', '7', '3'],
                     'user_id': ['1001', '1001', '1003', '1003', '1005', '1006', '1001', '1001'],
                     'enrollment_id': ['9010', '9020', '9030', '9040', '9050', '9060', '9070', '9080'],
                     'grade': ['A', '\\N', 'A', '\\N', 'B', 'C', '\\N', 'A'],
                     'department': ['AI  ', 'ML', '  CS', '  DS', '  AI', 'ML', '  CS', 'DS  ']}

course_enrolments_df = spark.createDataFrame(pd.DataFrame(course_enrolments))

# COMMAND ----------

def data_cleanse(c):
    return c.strip() if c.strip() != '\\N' else None

# COMMAND ----------

data_cleanse = spark.udf.register('data_cleanse', data_cleanse)

# COMMAND ----------

courses_df.show()

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

courses_df.select(
    data_cleanse(col('course_id')).alias('course_id'),
    data_cleanse(col('course_status').alias('course_status'))
).show()

# COMMAND ----------

courses_df.createOrReplaceTempView('courses')

# COMMAND ----------

spark.sql('''
    SELECT course_id, data_cleanse(course_status) AS course_status
    FROM courses
'''). \
    show()

# COMMAND ----------



#!/usr/bin/env python
# coding: utf-8

# # Spark SQL Examples
# 


from pyspark.sql import SparkSession

import datetime

spark = SparkSession \
    .builder \
    .appName("Data wrangling with Spark SQL") \
    .getOrCreate()


path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small_2.json"
user_log_df = spark.read.json(path)

user_log_df.take(1)

user_log_df.printSchema()


# # Create a View And Run Queries
# 
# The code below creates a temporary view against which you can run SQL queries.

user_log_df.createOrReplaceTempView("user_log_table")


spark.sql('''
          SELECT * 
          FROM user_log_table 
          LIMIT 2
          '''
          ).show()

spark.sql('''
          SELECT COUNT(*) 
          FROM user_log_table 
          '''
          ).show()

spark.sql('''
          SELECT userID, firstname, page, song
          FROM user_log_table 
          WHERE userID == '1046'
          '''
          ).show()

spark.sql('''
          SELECT DISTINCT page
          FROM user_log_table 
          ORDER BY page ASC
          '''
          ).show()


# # User Defined Functions

spark.udf.register("get_hour", lambda x: int(datetime.datetime.fromtimestamp(x / 1000.0).hour))

spark.sql('''
          SELECT *, get_hour(ts) AS hour
          FROM user_log_table 
          LIMIT 1
          '''
          ).show()

songs_in_hour_df = spark.sql('''
          SELECT get_hour(ts) AS hour, COUNT(*) as plays_per_hour
          FROM user_log_table
          WHERE page = "NextSong"
          GROUP BY hour
          ORDER BY cast(hour as int) ASC
          '''
          )

songs_in_hour_df.show()

# # Converting Results to Pandas

songs_in_hour_pd = songs_in_hour_df.toPandas()


print(songs_in_hour_pd)


#!/usr/bin/env python
# coding: utf-8

# # Answer Key to the Data Wrangling with DataFrames Coding Quiz
# 
# Helpful resources:
# https://spark.apache.org/docs/2.4.0/api/python/pyspark.sql.html


from pyspark.sql import SparkSession
from pyspark.sql.functions import col, desc, udf, col
from pyspark.sql.functions import sum as Fsum
from pyspark.sql.window import Window
from pyspark.sql.types import IntegerType

# 1) import any other libraries you might need
# 2) instantiate a Spark session 
# 3) read in the data set located at the path "../../data/sparkify_log_small.json"
# 4) write code to answer the quiz questions 

spark = SparkSession \
        .builder \
        .appName("Data Frames practice") \
        .getOrCreate()

logs_df = spark.read.json("./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json")


# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?


logs_df.printSchema()


# filter for users with blank user id
blank_pages_df = logs_df.filter(logs_df.userId == '') \
    .select(col('page') \
    .alias('blank_pages')) \
    .dropDuplicates()

# get a list of possible pages that could be visited
all_pages_df = logs_df.select('page').dropDuplicates()

# find values in all_pages that are not in blank_pages
# these are the pages that the blank user did not go to
# NOTE WE SHOULD NOT USE .collect() on large datasets (>100 MB)
for row in set(all_pages_df.collect()) - set(blank_pages_df.collect()):
    print(row.page)


# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# Perhaps it represents users who have not signed up yet or who are signed out and are about to log in.

# # Question 3
# 
# How many female users do we have in the data set?

print(
logs_df.filter(logs_df.gender == 'F')  \
    .select('userId', 'gender') \
    .dropDuplicates() \
    .count()
)

# # Question 4
# 
# How many songs were played from the most played artist?


logs_df.filter(logs_df.page == 'NextSong') \
    .select('Artist') \
    .groupBy('Artist') \
    .agg({'Artist':'count'}) \
    .withColumnRenamed('count(Artist)', 'Playcount') \
    .sort(desc('Playcount')) \
    .show(1)


# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

# TODO: filter out 0 sum and max sum to get more exact answer

user_window = Window \
    .partitionBy('userID') \
    .orderBy(desc('ts')) \
    .rangeBetween(Window.unboundedPreceding, 0)

ishome = udf(lambda ishome : int(ishome == 'Home'), IntegerType())

# Filter only NextSong and Home pages, add 1 for each time they visit Home
# Adding a column called period which is a specific interval between Home visits
cusum = logs_df.filter((logs_df.page == 'NextSong') | (logs_df.page == 'Home')) \
    .select('userID', 'page', 'ts') \
    .withColumn('homevisit', ishome(col('page'))) \
    .withColumn('period', Fsum('homevisit') \
    .over(user_window)) 
    
# This will only show 'Home' in the first several rows due to default sorting

cusum.show(300)


# See how many songs were listened to on average during each period
cusum.filter((cusum.page == 'NextSong')) \
    .groupBy('userID', 'period') \
    .agg({'period':'count'}) \
    .agg({'count(period)':'avg'}) \
    .show()




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


path = "/workspace/home/nd027-Data-Engineering-Data-Lakes-AWS-Exercises/lesson-2-spark-essentials/exercises/data/sparkify_log_small_2.json"
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

# # Question 1
# 
# Which page did user id "" (empty string) NOT visit?

# filter for users with blank user id
blank_pages_query = """
    SELECT DISTINCT page AS blank_pages
    FROM user_log_table
    WHERE userId = ''
"""

# get a list of possible pages that could be visited
all_pages_query = """
    SELECT DISTINCT page
    FROM user_log_table
"""

# find values in all_pages that are not in blank_pages
not_visited_pages_query = """
    SELECT all_pages.page
    FROM ({all_pages_query}) all_pages
    LEFT JOIN ({blank_pages_query}) blank_pages
    ON all_pages.page = blank_pages.blank_pages
    WHERE blank_pages.blank_pages IS NULL
""".format(all_pages_query=all_pages_query, blank_pages_query=blank_pages_query)

# Execute the queries
blank_pages_df = spark.sql(blank_pages_query)
all_pages_df = spark.sql(all_pages_query)
not_visited_pages_df = spark.sql(not_visited_pages_query)

# # Question 2 - Reflect
# 
# What type of user does the empty string user id most likely refer to?
# 

# Perhaps it represents users who have not signed up yet or who are signed out and are about to log in.

# # Question 3
# 
# How many female users do we have in the data set?

query = """
    SELECT COUNT(DISTINCT userId) AS count
    FROM user_log_table
    WHERE gender = 'F'
"""

# Execute the query
result = spark.sql(query)

# Print the result
result.show()

# # Question 4
# 
# How many songs were played from the most played artist?

query = """
    SELECT Artist, COUNT(Artist) AS Playcount
    FROM user_log_table
    WHERE page = 'NextSong'
    GROUP BY Artist
    ORDER BY Playcount DESC
    LIMIT 1
"""

# Execute the query
result = spark.sql(query)

# Print the result
result.show()

# # Question 5 (challenge)
# 
# How many songs do users listen to on average between visiting our home page? Please round your answer to the closest integer.
# 
# 

query = """
    WITH cusum AS (
        SELECT userID, page, ts,
            CASE WHEN page = 'Home' THEN 1 ELSE 0 END AS homevisit,
            SUM(CASE WHEN page = 'Home' THEN 1 ELSE 0 END) OVER (
                PARTITION BY userID
                ORDER BY ts DESC
                ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
            ) AS period
        FROM user_log_table
        WHERE page IN ('NextSong', 'Home')
    )
    SELECT AVG(song_count) AS average_songs
    FROM (
        SELECT userID, period, COUNT(period) AS song_count
        FROM cusum
        WHERE page = 'NextSong'
        GROUP BY userID, period
    )
"""

# Execute the query
result = spark.sql(query)

# Print the result
result.show()
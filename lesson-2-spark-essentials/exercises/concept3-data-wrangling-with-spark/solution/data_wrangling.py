# # Data Wrangling with Spark
# 
# This is the code used in the previous screencast. Run each code cell to understand what the code does and how it works.
# 
# These first three cells import libraries, instantiate a SparkSession, and then read in the data set

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import sum as Fsum

import datetime

import numpy as np
import pandas as pd
import matplotlib.pyplot as plt

spark = SparkSession \
    .builder \
    .appName("Wrangling Data") \
    .getOrCreate()

path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log_df = spark.read.json(path)


# # Data Exploration 
# 
# # Explore the data set.


# View 5 records 
print(
    user_log_df.take(5)
)
# Print the schema
user_log_df.printSchema()

# Describe the dataframe
user_log_df.describe().show()

# Describe the statistics for the song length column
user_log_df.describe("length").show()

# Count the rows in the dataframe
print(
    user_log_df.count()
)    

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()

# Select data for all pages where userId is 1046
user_log_df.select(["userId", "firstname", "page", "song"]) \
    .where(user_log_df.userId == "1046") \
    .show()


# # Calculating Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))

print(
    # Get the first row
    user_log_df.head(1)
)

# Select just the NextSong page
songs_in_hour_df = user_log_df.filter(user_log_df.page == "NextSong") \
    .groupby(user_log_df.hour) \
    .count() \
    .orderBy(user_log_df.hour.cast("float"))

songs_in_hour_df.show()

songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24)
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played")
plt.show()


# # Drop Rows with Missing Values
# 
# As you'll see, it turns out there are no missing values in the userID or session columns. But there are userID values that are empty strings.
# how = 'any' or 'all'. If 'any', drop a row if it contains any nulls. If 'all', drop a row only if all its values are null.
# subset = list of columns to consider
user_log_valid_df = user_log_df.dropna(how = "any", subset = ["userId", "sessionId"])

# How many are there now that we dropped rows with null userId or sessionId?
print(
user_log_valid_df.count()
)

# select all unique user ids into a dataframe
user_log_df.select("userId") \
    .dropDuplicates() \
    .sort("userId").show()

# Select only data for where the userId column isn't an empty string (different from null)
user_log_valid_df = user_log_valid_df.filter(user_log_valid_df["userId"] != "")

# Notice the count has dropped after dropping rows with empty userId
print(
    user_log_valid_df.count()
)

# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then show those log entries. 

user_log_valid_df.filter("page = 'Submit Downgrade'") \
    .show()

user_log_df.select(["userId", "firstname", "page", "level", "song"]) \
    .where(user_log_df.userId == "1138") \
    .show()

# Create a user defined function to return a 1 if the record contains a downgrade
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Select data including the user defined function
user_log_valid_df = user_log_valid_df \
    .withColumn("downgraded", flag_downgrade_event("page"))

print(
    user_log_valid_df.head()
)

from pyspark.sql import Window

# Partition by user id
# Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.
windowval = Window.partitionBy("userId") \
    .orderBy(desc("ts")) \
    .rangeBetween(Window.unboundedPreceding, 0)

# Fsum is a cumulative sum over a window - in this case a window showing all events for a user
# Add a column called phase, 0 if the user hasn't downgraded yet, 1 if they have
user_log_valid_df = user_log_valid_df \
    .withColumn("phase", Fsum("downgraded") \
    .over(windowval))

user_log_valid_df.show()    

# Show the phases for user 1138 
user_log_valid_df \
    .select(["userId", "firstname", "ts", "page", "level", "phase"]) \
    .where(user_log_df.userId == "1138") \
    .sort("ts") \
    .show()


# # Data Wrangling with Spark
# 
# This is the code used in the previous screencast. Run each code cell to understand what the code does and how it works.
# 
# These first three cells import libraries, instantiate a SparkSession, and then read in the data set

from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import desc
from pyspark.sql.functions import asc
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
user_log_df.take(5)

# Print the schema
user_log_df.printSchema()

# Describe the dataframe
user_log_df.describe().show()

# Describe the artist column
user_log_df.describe("artist").show()

# Describe the sessionId column
user_log_df.describe("sessionId").show()

# Count the rows in the dataframe
user_log_df.count()

# Select the page column, drop the duplicates, and sort by page
user_log_df.select("page").dropDuplicates().sort("page").show()

# Select several columns where userId is 1046
user_log_df.select(["userId", "firstname", "page", "song"]).where(user_log_df.userId == "1046").show()


# # Calculating Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

user_log_df = user_log_df.withColumn("hour", get_hour(user_log_df.ts))

user_log_df.head()

songs_in_hour_df = user_log_df.filter(user_log_df.page == "NextSong").groupby(user_log_df.hour).count().orderBy(user_log_df.hour.cast("float"))

songs_in_hour_df.show()

songs_in_hour_pd = songs_in_hour_df.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24);
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played");


# # Drop Rows with Missing Values
# 
# As you'll see, it turns out there are no missing values in the userID or session columns. But there are userID values that are empty strings.

user_log_valid_df = user_log_df.dropna(how = "any", subset = ["userId", "sessionId"])

user_log_valid_df.count()

user_log_df.select("userId").dropDuplicates().sort("userId").show()

user_log_valid_df = user_log_valid_df.filter(user_log_valid_df["userId"] != "")

user_log_valid_df.count()


# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then flag those log entries. Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.

user_log_valid_df.filter("page = 'Submit Downgrade'").show()

user_log_df.select(["userId", "firstname", "page", "level", "song"]).where(user_log_df.userId == "1138").show()

# Create a user defined function
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Select data using the user defined function
user_log_valid_df = user_log_valid_df.withColumn("downgraded", flag_downgrade_event("page"))

user_log_valid_df.head()

from pyspark.sql import Window

# Partition by user id
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)

user_log_valid_df = user_log_valid_df.withColumn("phase", Fsum("downgraded").over(windowval))

user_log_valid_df.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log_df.userId == "1138").sort("ts").show()


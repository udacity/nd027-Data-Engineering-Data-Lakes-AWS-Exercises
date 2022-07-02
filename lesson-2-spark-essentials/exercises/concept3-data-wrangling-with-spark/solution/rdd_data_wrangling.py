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
user_log_rdd = spark.read.json(path)


# # Data Exploration 
# 
# # Explore the data set.


# View 5 records 
user_log_rdd.take(5)

# Print the schema
user_log_rdd.printSchema()

# Describe the dataframe
user_log_rdd.describe().show()

# Describe the artist column
user_log_rdd.describe("artist").show()

# Describe the sessionId column
user_log_rdd.describe("sessionId").show()

# Count the rows in the dataframe
user_log_rdd.count()

# Select the page column, drop the duplicates, and sort by page
user_log_rdd.select("page").dropDuplicates().sort("page").show()

# Select several columns where userId is 1046
user_log_rdd.select(["userId", "firstname", "page", "song"]).where(user_log_rdd.userId == "1046").show()


# # Calculating Statistics by Hour
get_hour = udf(lambda x: datetime.datetime.fromtimestamp(x / 1000.0). hour)

user_log_rdd = user_log_rdd.withColumn("hour", get_hour(user_log_rdd.ts))

user_log_rdd.head()

songs_in_hour_rdd = user_log_rdd.filter(user_log_rdd.page == "NextSong").groupby(user_log_rdd.hour).count().orderBy(user_log_rdd.hour.cast("float"))

songs_in_hour_rdd.show()

songs_in_hour_pd = songs_in_hour_rdd.toPandas()
songs_in_hour_pd.hour = pd.to_numeric(songs_in_hour_pd.hour)

plt.scatter(songs_in_hour_pd["hour"], songs_in_hour_pd["count"])
plt.xlim(-1, 24);
plt.ylim(0, 1.2 * max(songs_in_hour_pd["count"]))
plt.xlabel("Hour")
plt.ylabel("Songs played");


# # Drop Rows with Missing Values
# 
# As you'll see, it turns out there are no missing values in the userID or session columns. But there are userID values that are empty strings.

user_log_valid_rdd = user_log_rdd.dropna(how = "any", subset = ["userId", "sessionId"])

user_log_valid_rdd.count()

user_log_rdd.select("userId").dropDuplicates().sort("userId").show()

user_log_valid_rdd = user_log_valid_rdd.filter(user_log_valid_rdd["userId"] != "")

user_log_valid_rdd.count()


# # Users Downgrade Their Accounts
# 
# Find when users downgrade their accounts and then flag those log entries. Then use a window function and cumulative sum to distinguish each user's data as either pre or post downgrade events.

user_log_valid_rdd.filter("page = 'Submit Downgrade'").show()

user_log_rdd.select(["userId", "firstname", "page", "level", "song"]).where(user_log_rdd.userId == "1138").show()

# Create a user defined function
flag_downgrade_event = udf(lambda x: 1 if x == "Submit Downgrade" else 0, IntegerType())

# Select data using the user defined function
user_log_valid_rdd = user_log_valid_rdd.withColumn("downgraded", flag_downgrade_event("page"))

user_log_valid_rdd.head()

from pyspark.sql import Window

# Partition by user id
windowval = Window.partitionBy("userId").orderBy(desc("ts")).rangeBetween(Window.unboundedPreceding, 0)

user_log_valid_rdd = user_log_valid_rdd.withColumn("phase", Fsum("downgraded").over(windowval))

user_log_valid_rdd.select(["userId", "firstname", "ts", "page", "level", "phase"]).where(user_log_rdd.userId == "1138").sort("ts").show()


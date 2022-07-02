import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pathlib import Path
import shutil


# Because we aren't running on a spark cluster, the session is just for development
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()


# This should print the default configuration
spark.sparkContext.getConf().getAll()

# This path resides on your computer or workspace, not in HDFS
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log_rdd = spark.read.json(path)

# See how Spark inferred the schema from the JSON file
user_log_rdd.printSchema()
user_log_rdd.describe()
user_log_rdd.show(n=1)
user_log_rdd.take(5)

# We are changing file formats
out_path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.csv"


# Delete CSV Directory if it already exists
# since the RDD API doesn't overwrite files by default, it would fail
csv_path = Path(out_path)
if csv_path.exists(): 
    shutil.rmtree(csv_path)

# The filename alone didn't tell Spark the actual format, we need to do it here
user_log_rdd.write.save(out_path, format="csv", header=True)

# This is just for fun to show the data that we wrote is readable
user_log_2_rdd = spark.read.csv(out_path, header=True)
user_log_2_rdd.printSchema()

# Choose two records from the CSV file
user_log_2_rdd.take(2)

# Show the userID column for all the rows
user_log_2_rdd.select("userID").show()

# 
user_log_2_rdd.take(1)
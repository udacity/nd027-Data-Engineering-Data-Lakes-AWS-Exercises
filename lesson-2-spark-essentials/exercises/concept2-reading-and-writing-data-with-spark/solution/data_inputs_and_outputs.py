import pyspark
from pyspark import SparkConf
from pyspark.sql import SparkSession


# Because we aren't running on a spark cluster, the session is just for development
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()


# This should print the default configuration
spark.sparkContext.getConf().getAll()

# This path resides on your computer or workspace, not in HDFS
path = "../../data/sparkify_log_small.json"
user_log = spark.read.json(path)

# See how Spark inferred the schema from the JSON file
user_log.printSchema()
user_log.describe()
user_log.show(n=1)
user_log.take(5)

# We are changing file formats
out_path = "../../data/sparkify_log_small.csv"

# The filename alone didn't tell Spark the actual format, we need to do it here
user_log.write.save(out_path, format="csv", header=True)

# This is just for fun to show the data that we wrote is readable
user_log_2 = spark.read.csv(out_path, header=True)
user_log_2.printSchema()

# Choose two records from the CSV file
user_log_2.take(2)

# Show the userID column for all the rows
user_log_2.select("userID").show()

# 
user_log_2.take(1)
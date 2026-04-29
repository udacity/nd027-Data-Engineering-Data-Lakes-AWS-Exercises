from pyspark.sql import SparkSession


# Because we aren't running on a spark cluster, the session is just for development
spark = SparkSession \
    .builder \
    .appName("Our first Python Spark SQL example") \
    .getOrCreate()


# This should print the default configuration
print(
    spark.sparkContext.getConf().getAll()
)    

# This path resides on your computer or workspace, not in HDFS
path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.json"
user_log_df = spark.read.json(path)

# See how Spark inferred the schema from the JSON file
user_log_df.printSchema()
print(
    user_log_df.describe()
)

user_log_df.show(n=1)
print(
    user_log_df.take(5)
)

# We are changing file formats
out_path = "./lesson-2-spark-essentials/exercises/data/sparkify_log_small.csv"



# The filename alone didn't tell Spark the actual format, we need to do it here
user_log_df.write.mode("overwrite").save(out_path, format="csv", header=True)

# Notice we have created another dataframe here
# We wouldn't usually read the data that we just wrote
# This does show, however, that the read method works with
# Different data types
user_log_2_df = spark.read.csv(out_path, header=True)
user_log_2_df.printSchema()

# Choose two records from the CSV file
print(
    user_log_2_df.take(2)
)    

# Show the userID column for the first several rows
user_log_2_df.select("userID").show()

# 
print(
user_log_2_df.take(1)
)
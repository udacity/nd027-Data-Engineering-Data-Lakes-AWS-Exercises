### Using S3 Buckets to Store Data

S3 stores an object, and when you identify an object, you need to specify a bucket, and key to identify the object.
For example,

```
df = spark.read.load(“s3://my_bucket/path/to/file/file.csv”)
```

From this code, `s3://my_bucket`is the bucket, and `path/to/file/file.csv` is the key for the object. Thankfully, if we’re using spark, and all the objects underneath the bucket have the same schema, you can do something like below.

```
df = spark.read.load(“s3://my_bucket/”)
```

This will generate a dataframe of all the objects underneath the `my_bucket` with the same schema.
Imagine you have a structure in S3 like this:

```
my_bucket
  |---test.csv
  path/to/
     |--test2.csv
     file/
       |--test3.csv
       |--file.csv
```

If all the csv files underneath `my_bucket`, which are `test.csv`, `test2.csv`, `test3.csv`, and `file.csv` have the same schema, the dataframe will be generated without error, but if there are conflicts in schema between files, then the dataframe will not be generated.
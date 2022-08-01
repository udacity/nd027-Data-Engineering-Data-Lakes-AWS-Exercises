Using the AWS CLI, copy the accelerometer data into an S3 landing zone with the `s3 cp` command (where the blank is the S3 bucket you created earlier):

`aws s3 cp ./project/starter/accelerometer/ \ s3://_______/accelerometer/landing/ --recursive`

Using the AWS CLI list the data in the landing zone (where the blank is the bucket name you created earlier in the course):

```
aws s3 ls s3://_______/accelerometer/landing/
```

- Go to Athena, select the database, and create a new table from S3 bucket data
- Name the table something appropriate for the landing zone, choose the database, and enter a path ending with a slash
- Choose the JSON data format, and add column names and types
- Preview the create table query and click **create table**
- Copy the table query, and save it as accelerometer_landing.sql, then push it to your GitHub repository

# Query Some Sample Data

Now run a query like: `select * from accelerometer_landing`

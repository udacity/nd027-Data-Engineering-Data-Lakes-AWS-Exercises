# Ingesting Sensitive Data

Before we can process the sensitive accelerometer data, we need to bring it into the **landing zone**.

Using the AWS CLI, copy the accelerometer data into an S3 landing zone with the `s3 cp` command (where the blank is the S3 bucket you created earlier):

`aws s3 cp ./project/starter/accelerometer/ \ s3://_______/accelerometer/landing/ --recursive`

*Hint: S3 folders are dynamically generated when you copy data to them (even if they don't exist yet)*

Using the AWS CLI list the data in the landing zone (where the blank is the bucket name you created earlier in the course):

```
aws s3 ls s3://_______/accelerometer/landing/
```
# Handling Sensitive Data

Data in the landing zone should be dealt with very carefully. It shouldn't be made available for analytics or business intelligence.

# Define a Glue Table for the Landing Zone

Now that you have some data in the landing zone you can define a glue table to make ad hoc querying easier. The landing zone should not be used for reporting or analytics, since the data is not  qualified to be used for those purposes. However, you may want to get an idea of what the data looks like:

Look at a sample JSON record:

```
{
    "user":"Christina.Huey@test.com","timeStamp":1655296571764,
    "x":-1.0,
    "y":-1.0,
    "z":-1.0
}
```

Following the same steps used to create the customer landing zone, repeat to create the accelerometer landing zone.

# Query Some Sample Data

Now run a query like: `select * from accelerometer_landing`

What fields contain **Personally Identifiable Information (PII) **in this data?

<br data-md>

As you looked at some sample data, did you see any fields that had invalid data?
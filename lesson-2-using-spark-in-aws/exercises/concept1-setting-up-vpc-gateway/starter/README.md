
# Creating an S3 Bucket

Use the AWS CLI to create the S3 bucket you will use for the remainder of the course. These need to be unique across all AWS accounts, so be creative!

Write a `createbucket.sh` script to save the command you used to create your S3 bucket. This will allow you to easily recall the command later after the course.


To create an S3 bucket use the `aws s3 mb` command: `aws s3 mb s3://_______` replacing the blank with the name of your bucket. 

# S3 Gateway

By default, Glue Jobs can't reach any networks outside of your Virtual Private Cloud (VPC). Since the S3 Service runs in a network outside of the VPC, we need to create what's called an S3 Gateway Endpoint. This allows S3 traffic from your Glue Jobs into your S3 buckets. Once we have created the endpoint, your Glue Jobs will have a network path to reach S3.


Write a `creategateway.sh` script to create the S3 Gateway, replacing the blanks with the **VPC** and **Routing Table Ids**:

<br data-md>

`aws ec2 create-vpc-endpoint --vpc-id _______ --service-name com.amazonaws.region.s3 \
  --route-table-ids _______`





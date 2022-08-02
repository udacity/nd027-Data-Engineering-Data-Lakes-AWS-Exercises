# Glue Studio

We started off by introducing Spark, then RDDs, and then Spark SQL. Spark SQL relies on RDDs, which rely on Spark itself. Similarly, Glue is an AWS Service that relies on Spark. You can use Glue Studio to write purely Spark scripts. We will also go over unique Glue features you can add to your script.


# Creating an S3 Bucket

Buckets are storage locations within AWS, that have a hierarchical directory-like structure. Once you create an S3 bucket, you can create as many sub-directories, and files as you want. The bucket is the "parent" of all of these directories and files.



<br data-md>

To create an S3 bucket use the `aws s3 mb` command: `aws s3 mb s3://_______` replacing the blank with the name of your bucket. 

# S3 Gateway

By default, Glue Jobs can't reach any networks outside of your Virtual Private Cloud (VPC). Since the S3 Service runs in a network outside of the VPC, we need to create what's called an S3 Gateway Endpoint. This allows S3 traffic from your Glue Jobs into your S3 buckets. Once we have created the endpoint, your Glue Jobs will have a network path to reach S3.

<br data-md>

First use the AWS CLI to identify the VPC that needs access to S3:

<br data-md>

`aws ec2 describe-vpcs`

<br data-md>

The output should looks something like this (look for the VpcId in the output):

```
{
    "Vpcs": [
        {
            "CidrBlock": "172.31.0.0/16",
            "DhcpOptionsId": "dopt-756f580c",
            "State": "available",
            "VpcId": "vpc-7385c60b",
            "OwnerId": "863507759259",
            "InstanceTenancy": "default",
            "CidrBlockAssociationSet": [
                {
                    "AssociationId": "vpc-cidr-assoc-664c0c0c",
                    "CidrBlock": "172.31.0.0/16",
                    "CidrBlockState": {
                        "State": "associated"
                    }
                }
            ],
            "IsDefault": true
        }
    ]
}
```

Next, identify the routing table (look for the RouteTableId):

`aws ec2 describe-route-tables`

```
{
    "RouteTables": [

        {
      . . .
            "PropagatingVgws": [],
            "RouteTableId": "rtb-bc5aabc1",
            "Routes": [
                {
                    "DestinationCidrBlock": "172.31.0.0/16",
                    "GatewayId": "local",
                    "Origin": "CreateRouteTable",
                    "State": "active"
                }
            ],
            "Tags": [],
            "VpcId": "vpc-7385c60b",
            "OwnerId": "863507759259"
        }
    ]
}

```

**Hint: If the RouteTables come back empty: [], go to the Route Tables Page in AWS, and it will generate one automatically**

<br data-md>

Finally create the S3 Gateway, replacing the blanks with the **VPC** and **Routing Table Ids**:

<br data-md>

`aws ec2 create-vpc-endpoint --vpc-id _______ --service-name com.amazonaws.us-east-1.s3 \
  --route-table-ids _______`



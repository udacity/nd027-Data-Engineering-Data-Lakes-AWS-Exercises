#!/bin/bash

# Create the S3 Gateway, replacing the blanks with the VPC and Routing Table Ids: aws ec2 create-vpc-endpoint --vpc-id _______ --service-name com.amazonaws.region.s3 \ --route-table-ids _______

aws ec2 create-vpc-endpoint --vpc-id vpc-7385c60b --service-name com.amazonaws.region.s3  --route-table-ids rtb-bc5aabc1
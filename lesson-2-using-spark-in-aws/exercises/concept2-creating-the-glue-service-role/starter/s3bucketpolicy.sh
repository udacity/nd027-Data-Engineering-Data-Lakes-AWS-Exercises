#!/bin/bash

#  allow your Glue job read/write/delete access to the bucket and everything in it. 

# 
# aws iam put-role-policy --role-name my-glue-service-role --policy-name S3Access --policy-document '{
#     "Version": "2012-10-17",
#     "Statement": [
#         {
#             "Sid": "ListObjectsInBucket",
#             "Effect": "Allow",
#             "Action": [
#                 "s3:ListBucket"
#             ],
#             "Resource": [
#                 "arn:aws:s3:::_______"
#             ]
#         },
#         {
#             "Sid": "AllObjectActions",
#             "Effect": "Allow",
#             "Action": "s3:*Object",
#             "Resource": [
#                 "arn:aws:s3:::_______/*"
#             ]
#         }
#     ]
# }'
# 



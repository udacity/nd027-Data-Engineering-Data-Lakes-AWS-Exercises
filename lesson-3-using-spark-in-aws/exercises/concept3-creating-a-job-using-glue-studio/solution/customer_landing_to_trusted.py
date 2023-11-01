import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import re

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Landing
CustomerLanding_node1698072415085 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://stedi-udacity/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1698072415085",
)

# Script generated for node Share With Research
ShareWithResearch_node1698176517721 = Filter.apply(
    frame=CustomerLanding_node1698072415085,
    f=lambda row: (not (row["shareWithResearchAsOfDate"] == 0)),
    transformation_ctx="ShareWithResearch_node1698176517721",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1698072645636 = glueContext.write_dynamic_frame.from_options(
    frame=ShareWithResearch_node1698176517721,
    connection_type="s3",
    format="json",
    connection_options={
        "path": "s3://stedi-udacity/customer/trusted/",
        "partitionKeys": [],
    },
    transformation_ctx="CustomerTrusted_node1698072645636",
)

job.commit()

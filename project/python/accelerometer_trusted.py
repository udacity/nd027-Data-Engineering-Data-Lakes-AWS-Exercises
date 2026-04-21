import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# Script generated for node Amazon S3
AmazonS3_node1776719742716 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_trusted", transformation_ctx="AmazonS3_node1776719742716")

# Script generated for node Amazon S3
AmazonS3_node1776719746700 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_landing", transformation_ctx="AmazonS3_node1776719746700")

# Script generated for node Join
Join_node1776719822011 = Join.apply(frame1=AmazonS3_node1776719742716, frame2=AmazonS3_node1776719746700, keys1=["email"], keys2=["user"], transformation_ctx="Join_node1776719822011")

# Script generated for node Drop Fields
DropFields_node1776719872803 = DropFields.apply(frame=Join_node1776719822011, paths=["customername", "email", "phone", "birthday", "serialnumber", "registrationdate", "lastupdatedate", "sharewithresearchasofdate", "sharewithpublicasofdate", "sharewithfriendsasofdate"], transformation_ctx="DropFields_node1776719872803")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1776719872803, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776719736267", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776720294958 = glueContext.getSink(path="s3://d609-stedi-lake/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776720294958")
AmazonS3_node1776720294958.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="accelerometer_trusted")
AmazonS3_node1776720294958.setFormat("json")
AmazonS3_node1776720294958.writeFrame(DropFields_node1776719872803)
job.commit()

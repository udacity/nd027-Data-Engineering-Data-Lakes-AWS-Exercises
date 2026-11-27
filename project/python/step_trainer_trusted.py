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
AmazonS3_node1776724764023 = glueContext.create_dynamic_frame.from_options(format_options={"multiLine": "false"}, connection_type="s3", format="json", connection_options={"paths": ["s3://d609-stedi-lake/step_trainer/landing/"], "recurse": True}, transformation_ctx="AmazonS3_node1776724764023")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776724763308 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_curated", transformation_ctx="AWSGlueDataCatalog_node1776724763308")

# Script generated for node Change Schema
ChangeSchema_node1776725524014 = ApplyMapping.apply(frame=AmazonS3_node1776724764023, mappings=[("sensorreadingtime", "bigint", "sensorreadingtime", "long"), ("serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "int")], transformation_ctx="ChangeSchema_node1776725524014")

# Script generated for node Change Schema
ChangeSchema_node1776725497485 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1776724763308, mappings=[("serialnumber", "string", "serialnumber", "string"), ("birthday", "string", "birthday", "string"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("email", "string", "email", "string"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("phone", "string", "phone", "string")], transformation_ctx="ChangeSchema_node1776725497485")

# Script generated for node Join
Join_node1776724808819 = Join.apply(frame1=ChangeSchema_node1776725497485, frame2=ChangeSchema_node1776725524014, keys1=["serialnumber"], keys2=["serialnumber"], transformation_ctx="Join_node1776724808819")

# Script generated for node Drop Fields
DropFields_node1776724911497 = DropFields.apply(frame=Join_node1776724808819, paths=["phone", "lastupdatedate", "email", "sharewithfriendsasofdate", "customername", "registrationdate", "sharewithresearchasofdate", "sharewithpublicasofdate", "birthday", "`.serialnumber`"], transformation_ctx="DropFields_node1776724911497")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=DropFields_node1776724911497, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776724254062", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776725080695 = glueContext.getSink(path="s3://d609-stedi-lake/step_trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776725080695")
AmazonS3_node1776725080695.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="step_trainer_trusted")
AmazonS3_node1776725080695.setFormat("json")
AmazonS3_node1776725080695.writeFrame(DropFields_node1776724911497)
job.commit()

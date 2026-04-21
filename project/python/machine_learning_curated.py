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

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776727951446 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="accelerometer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776727951446")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1776727952045 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="step_trainer_trusted", transformation_ctx="AWSGlueDataCatalog_node1776727952045")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1776728217685 = ApplyMapping.apply(frame=AWSGlueDataCatalog_node1776727952045, mappings=[("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "right_distancefromobject", "int"), ("sensorreadingtime", "long", "right_sensorreadingtime", "long")], transformation_ctx="RenamedkeysforJoin_node1776728217685")

# Script generated for node Join
Join_node1776727972332 = Join.apply(frame1=AWSGlueDataCatalog_node1776727951446, frame2=RenamedkeysforJoin_node1776728217685, keys1=["timestamp"], keys2=["right_sensorreadingtime"], transformation_ctx="Join_node1776727972332")

# Script generated for node Drop Fields
DropFields_node1776728117832 = DropFields.apply(frame=Join_node1776727972332, paths=["user", "timestamp"], transformation_ctx="DropFields_node1776728117832")

# Script generated for node Change Schema
ChangeSchema_node1776728359996 = ApplyMapping.apply(frame=DropFields_node1776728117832, mappings=[("serialnumber", "string", "serialnumber", "string"), ("z", "double", "z", "double"), ("birthday", "string", "birthday", "string"), ("sharewithpublicasofdate", "long", "sharewithpublicasofdate", "long"), ("sharewithresearchasofdate", "long", "sharewithresearchasofdate", "long"), ("registrationdate", "long", "registrationdate", "long"), ("customername", "string", "customername", "string"), ("y", "double", "y", "double"), ("sharewithfriendsasofdate", "long", "sharewithfriendsasofdate", "long"), ("x", "double", "x", "double"), ("lastupdatedate", "long", "lastupdatedate", "long"), ("right_serialnumber", "string", "serialnumber", "string"), ("right_distancefromobject", "int", "distancefromobject", "int"), ("right_sensorreadingtime", "long", "sensorreadingtime", "long")], transformation_ctx="ChangeSchema_node1776728359996")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=ChangeSchema_node1776728359996, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776725907533", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776728433573 = glueContext.getSink(path="s3://d609-stedi-lake/step_trainer/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776728433573")
AmazonS3_node1776728433573.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="machine_learning_curated")
AmazonS3_node1776728433573.setFormat("json")
AmazonS3_node1776728433573.writeFrame(ChangeSchema_node1776728359996)
job.commit()

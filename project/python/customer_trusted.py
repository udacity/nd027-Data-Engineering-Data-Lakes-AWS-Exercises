import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue import DynamicFrame

def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)
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
AmazonS3_node1776717520359 = glueContext.create_dynamic_frame.from_catalog(database="stedi_database", table_name="customer_landing", transformation_ctx="AmazonS3_node1776717520359")

# Script generated for node Privacy Filter
SqlQuery0 = '''
select * from myDataSource
where sharewithresearchasofdate is not null
'''
PrivacyFilter_node1776717592722 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":AmazonS3_node1776717520359}, transformation_ctx = "PrivacyFilter_node1776717592722")

# Script generated for node Amazon S3
EvaluateDataQuality().process_rows(frame=PrivacyFilter_node1776717592722, ruleset=DEFAULT_DATA_QUALITY_RULESET, publishing_options={"dataQualityEvaluationContext": "EvaluateDataQuality_node1776717408866", "enableDataQualityResultsPublishing": True}, additional_options={"dataQualityResultsPublishing.strategy": "BEST_EFFORT", "observations.scope": "ALL"})
AmazonS3_node1776717776208 = glueContext.getSink(path="s3://d609-stedi-lake/customer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1776717776208")
AmazonS3_node1776717776208.setCatalogInfo(catalogDatabase="stedi_database",catalogTableName="customer_trusted")
AmazonS3_node1776717776208.setFormat("json")
AmazonS3_node1776717776208.writeFrame(PrivacyFilter_node1776717592722)
job.commit()

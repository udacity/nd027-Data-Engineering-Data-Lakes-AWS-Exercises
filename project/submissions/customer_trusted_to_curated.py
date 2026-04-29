import sys
from awsglue.transforms import Join
from awsglue.dynamicframe import DynamicFrame
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database='stedi_db',
    table_name='customer_trusted',
    transformation_ctx='customer_trusted'
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database='stedi_db',
    table_name='accelerometer_trusted',
    transformation_ctx='accelerometer_trusted'
)

joined = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_trusted,
    keys1=['email'],
    keys2=['user'],
    transformation_ctx='customer_trusted_join_accelerometer'
)

curated = DynamicFrame.fromDF(
    joined.toDF().dropDuplicates(['serialNumber']),
    glueContext,
    'customer_curated'
)

curated_sink = glueContext.getSink(
    path='s3://s3spspark/customer/curated/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    compression='snappy',
    enableUpdateCatalog=True,
    transformation_ctx='customer_curated_sink'
)
curated_sink.setCatalogInfo(catalogDatabase='stedi_db', catalogTableName='customer_curated')
curated_sink.setFormat('json')
curated_sink.writeFrame(curated)

job.commit()

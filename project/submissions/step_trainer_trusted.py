import sys
from awsglue.transforms import Join
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

step_trainer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    format='json',
    connection_options={'paths': ['s3://s3spspark/step_trainer/landing/']},
    format_options={'multiLine': False},
    transformation_ctx='step_trainer_landing'
)

customer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database='stedi_db',
    table_name='customer_trusted',
    transformation_ctx='customer_trusted'
)

joined = Join.apply(
    frame1=step_trainer_landing,
    frame2=customer_trusted,
    keys1=['serialNumber'],
    keys2=['serialNumber'],
    transformation_ctx='step_trainer_trusted_join'
)

trusted_sink = glueContext.getSink(
    path='s3://s3spspark/step_trainer/trusted/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    compression='snappy',
    enableUpdateCatalog=True,
    transformation_ctx='step_trainer_trusted_sink'
)
trusted_sink.setCatalogInfo(catalogDatabase='stedi_db', catalogTableName='step_trainer_trusted')
trusted_sink.setFormat('json')
trusted_sink.writeFrame(joined)

job.commit()

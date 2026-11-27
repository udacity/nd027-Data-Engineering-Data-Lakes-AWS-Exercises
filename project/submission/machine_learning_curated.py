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

step_trainer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database='stedi_db',
    table_name='step_trainer_trusted',
    transformation_ctx='step_trainer_trusted'
)

accelerometer_trusted = glueContext.create_dynamic_frame.from_catalog(
    database='stedi_db',
    table_name='accelerometer_trusted',
    transformation_ctx='accelerometer_trusted'
)

ml_curated = Join.apply(
    frame1=step_trainer_trusted,
    frame2=accelerometer_trusted,
    keys1=['sensorReadingTime'],
    keys2=['timeStamp'],
    transformation_ctx='machine_learning_curated_join'
)

ml_curated_sink = glueContext.getSink(
    path='s3://s3spspark/machine_learning/curated/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    compression='snappy',
    enableUpdateCatalog=True,
    transformation_ctx='machine_learning_curated_sink'
)
ml_curated_sink.setCatalogInfo(catalogDatabase='stedi_db', catalogTableName='machine_learning_curated')
ml_curated_sink.setFormat('json')
ml_curated_sink.writeFrame(ml_curated)

job.commit()

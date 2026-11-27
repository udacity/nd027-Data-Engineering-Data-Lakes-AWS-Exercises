import sys
from awsglue.transforms import Join, DropFields
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

accelerometer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    format='json',
    connection_options={'paths': ['s3://s3spspark/accelerometer/landing/']},
    format_options={'multiLine': False},
    transformation_ctx='accelerometer_landing'
)

joined = Join.apply(
    frame1=customer_trusted,
    frame2=accelerometer_landing,
    keys1=['email'],
    keys2=['user'],
    transformation_ctx='customer_accelerometer_join'
)

trusted = DropFields.apply(
    frame=joined,
    paths=[
        'serialNumber',
        'birthDay',
        'shareWithPublicAsOfDate',
        'shareWithResearchAsOfDate',
        'registrationDate',
        'customerName',
        'shareWithFriendsAsOfDate',
        'email',
        'lastUpdateDate',
        'phone'
    ],
    transformation_ctx='accelerometer_trusted_drop_fields'
)

trusted_sink = glueContext.getSink(
    path='s3://s3spspark/accelerometer/trusted/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    compression='snappy',
    enableUpdateCatalog=True,
    transformation_ctx='accelerometer_trusted_sink'
)
trusted_sink.setCatalogInfo(catalogDatabase='stedi_db', catalogTableName='accelerometer_trusted')
trusted_sink.setFormat('json')
trusted_sink.writeFrame(trusted)

job.commit()

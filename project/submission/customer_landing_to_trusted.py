import sys
from awsglue.transforms import Filter
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

customer_landing = glueContext.create_dynamic_frame.from_options(
    connection_type='s3',
    format='json',
    connection_options={
        'paths': ['s3://s3spspark/customer/landing/'],
        'recurse': True
    },
    format_options={'multiLine': False},
    transformation_ctx='customer_landing'
)

customer_trusted = Filter.apply(
    frame=customer_landing,
    f=lambda row: row.get('shareWithResearchAsOfDate') is not None,
    transformation_ctx='customer_trusted_filter'
)

customer_trusted_sink = glueContext.getSink(
    path='s3://s3spspark/customer/trusted/',
    connection_type='s3',
    updateBehavior='UPDATE_IN_DATABASE',
    partitionKeys=[],
    compression='snappy',
    enableUpdateCatalog=True,
    transformation_ctx='customer_trusted_sink'
)
customer_trusted_sink.setCatalogInfo(catalogDatabase='stedi_db', catalogTableName='customer_trusted')
customer_trusted_sink.setFormat('json')
customer_trusted_sink.writeFrame(customer_trusted)

job.commit()

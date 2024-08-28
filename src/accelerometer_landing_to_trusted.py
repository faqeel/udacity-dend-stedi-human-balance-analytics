import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
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

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1724805040019 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_landing", transformation_ctx="AccelerometerLanding_node1724805040019")

# Script generated for node Customer Trusted
CustomerTrusted_node1724805061903 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_trusted", transformation_ctx="CustomerTrusted_node1724805061903")

# Script generated for node Join
Join_node1724805080165 = Join.apply(frame1=AccelerometerLanding_node1724805040019, frame2=CustomerTrusted_node1724805061903, keys1=["user"], keys2=["email"], transformation_ctx="Join_node1724805080165")

# Script generated for node Drop Fields
SqlQuery0 = '''
select user, timestamp, x, y, z from myDataSource
'''
DropFields_node1724805093096 = sparkSqlQuery(glueContext, query = SqlQuery0, mapping = {"myDataSource":Join_node1724805080165}, transformation_ctx = "DropFields_node1724805093096")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724805133181 = glueContext.getSink(path="s3://stedi-hba-lh/accelerometer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="AccelerometerTrusted_node1724805133181")
AccelerometerTrusted_node1724805133181.setCatalogInfo(catalogDatabase="stedi",catalogTableName="accelerometer_trusted")
AccelerometerTrusted_node1724805133181.setFormat("json")
AccelerometerTrusted_node1724805133181.writeFrame(DropFields_node1724805093096)
job.commit()
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724808190441 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_trusted", transformation_ctx="StepTrainerTrusted_node1724808190441")

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1724808247888 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="accelerometer_trusted", transformation_ctx="AccelerometerTrusted_node1724808247888")

# Script generated for node Join
Join_node1724808269817 = Join.apply(frame1=StepTrainerTrusted_node1724808190441, frame2=AccelerometerTrusted_node1724808247888, keys1=["sensorreadingtime"], keys2=["timestamp"], transformation_ctx="Join_node1724808269817")

# Script generated for node Drop Fields
DropFields_node1724808533660 = DropFields.apply(frame=Join_node1724808269817, paths=["user", "timestamp", "serialnumber"], transformation_ctx="DropFields_node1724808533660")

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1724808429650 = glueContext.getSink(path="s3://stedi-hba-lh/machine-learning/curated/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="MachineLearningCurated_node1724808429650")
MachineLearningCurated_node1724808429650.setCatalogInfo(catalogDatabase="stedi",catalogTableName="machine_learning_curated")
MachineLearningCurated_node1724808429650.setFormat("json")
MachineLearningCurated_node1724808429650.writeFrame(DropFields_node1724808533660)
job.commit()
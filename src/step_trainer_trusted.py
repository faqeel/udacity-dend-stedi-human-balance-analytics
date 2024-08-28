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

# Script generated for node Customer Curated
CustomerCurated_node1724807510714 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="customer_curated", transformation_ctx="CustomerCurated_node1724807510714")

# Script generated for node Step Trainer Landing
StepTrainerLanding_node1724807601723 = glueContext.create_dynamic_frame.from_catalog(database="stedi", table_name="step_trainer_landing", transformation_ctx="StepTrainerLanding_node1724807601723")

# Script generated for node Renamed keys for Join
RenamedkeysforJoin_node1724807636042 = ApplyMapping.apply(frame=StepTrainerLanding_node1724807601723, mappings=[("sensorreadingtime", "long", "right_sensorreadingtime", "long"), ("serialnumber", "string", "right_serialnumber", "string"), ("distancefromobject", "int", "distancefromobject", "long")], transformation_ctx="RenamedkeysforJoin_node1724807636042")

# Script generated for node Join
Join_node1724807124636 = Join.apply(frame1=CustomerCurated_node1724807510714, frame2=RenamedkeysforJoin_node1724807636042, keys1=["serialnumber"], keys2=["right_serialnumber"], transformation_ctx="Join_node1724807124636")

# Script generated for node Change Schema
ChangeSchema_node1724807274256 = ApplyMapping.apply(frame=Join_node1724807124636, mappings=[("right_sensorreadingtime", "long", "sensorreadingtime", "long"), ("right_serialnumber", "string", "serialnumber", "string"), ("distancefromobject", "long", "distancefromobject", "long")], transformation_ctx="ChangeSchema_node1724807274256")

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1724807799770 = glueContext.getSink(path="s3://stedi-hba-lh/step-trainer/trusted/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="StepTrainerTrusted_node1724807799770")
StepTrainerTrusted_node1724807799770.setCatalogInfo(catalogDatabase="stedi",catalogTableName="step_trainer_trusted")
StepTrainerTrusted_node1724807799770.setFormat("json")
StepTrainerTrusted_node1724807799770.writeFrame(ChangeSchema_node1724807274256)
job.commit()
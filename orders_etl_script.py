# orders_etl_script.py
# AWS Glue ETL script: reads from Glue Data Catalog (DynamoDB via crawler),
# filters shipped orders and writes Parquet to S3.
#
# Usage: create Glue Job, paste this script, set Job name and IAM role with
# required permissions (AWSGlueServiceRole, AmazonDynamoDBReadOnlyAccess, AmazonS3FullAccess).
# Update GLUE_DATABASE, GLUE_TABLE and S3_OUTPUT_PATH variables as needed.

import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from pyspark.sql.functions import lower, col
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.dynamicframe import DynamicFrame

# Get job name from arguments (Glue passes JOB_NAME automatically)
args = getResolvedOptions(sys.argv, ['JOB_NAME'])
job_name = args['JOB_NAME']

sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(job_name, args)

# -------------------------
# CONFIG - update these
# -------------------------
GLUE_DATABASE = "ordersdb"              # Glue database created by crawler
GLUE_TABLE = "orders"                   # Glue table name created by crawler (or use orders_shipped if crawler created that)
S3_OUTPUT_PATH = "s3://my-orders-bucket8483/shipped/"  # change to your S3 bucket/path
# -------------------------

# Read data from Glue Data Catalog (the crawler must have created the table)
datasource = glueContext.create_dynamic_frame.from_catalog(
    database=GLUE_DATABASE,
    table_name=GLUE_TABLE,
    transformation_ctx="datasource"
)

# Convert DynamicFrame to Spark DataFrame for easier SQL-like filtering
df = datasource.toDF()

# Make sure 'status' column exists; apply filter for shipped (case-insensitive)
if 'status' in df.columns:
    df_filtered = df.filter(lower(col('status')) == 'shipped')
else:
    # If status column missing, create empty dataframe with same schema (safe fallback)
    df_filtered = df.limit(0)

# (Optional) - show schema and some rows in CloudWatch logs (for debugging)
print("Schema of incoming data:")
df.printSchema()
print("Number of records before filter: {}".format(df.count()))
print("Number of records after filter: {}".format(df_filtered.count()))

# Convert back to DynamicFrame
dynamic_frame_filtered = DynamicFrame.fromDF(df_filtered, glueContext, "dynamic_frame_filtered")

# Write filtered data to S3 in Parquet format (partitioning optional)
glueContext.write_dynamic_frame.from_options(
    frame=dynamic_frame_filtered,
    connection_type="s3",
    connection_options={
        "path": S3_OUTPUT_PATH,
        # Uncomment to partition by a column (e.g., OrderDate) if needed:
        # "partitionKeys": ["OrderDate"]
    },
    format="parquet",
    transformation_ctx="s3sink"
)

job.commit()

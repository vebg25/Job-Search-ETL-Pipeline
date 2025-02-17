import sys
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, concat_ws
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType, DoubleType

# Get job arguments
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'input_path', 'output_path'])

# Initialize Glue & Spark Context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Define Schema
schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("employer_name", StringType(), True),
    StructField("job_description", StringType(), True),
    StructField("job_publisher", StringType(), True),
    StructField("job_employment_type", StringType(), True),
    StructField("job_apply_link", StringType(), True),
    StructField("job_apply_is_direct", BooleanType(), True),
    StructField("job_is_remote", BooleanType(), True),
    StructField("job_posted_at", StringType(), True),
    StructField("job_city", StringType(), True),
    StructField("job_state", StringType(), True),
    StructField("job_country", StringType(), True),
    StructField("job_salary", StringType(), True),
    StructField("job_min_salary", StringType(), True),
    StructField("job_max_salary", StringType(), True),
    StructField("job_highlights", StructType([
        StructField("Qualifications", ArrayType(StringType()), True),
        StructField("Responsibilities", ArrayType(StringType()), True)
    ]), True)
])

# Read data from S3 as DataFrame
df = spark.read.option("multiline", "true").json(args['input_path'], schema=schema)

# Extract required columns and transform data
df_final = df.select(
    col("job_id"),
    col("job_title"),
    col("employer_name"),
    col("job_description"),
    col("job_publisher"),
    col("job_employment_type"),
    col("job_apply_link"),
    col("job_apply_is_direct"),
    col("job_is_remote"),
    col("job_posted_at"),
    col("job_city"),
    col("job_state"),
    col("job_country"),
    
    # Convert salary fields to DoubleType
    col("job_salary"),
    col("job_min_salary").cast(DoubleType()).alias("job_min_salary"),
    col("job_max_salary").cast(DoubleType()).alias("job_max_salary"),

    # Flatten array fields
    concat_ws(" ", col("job_highlights.Qualifications")).alias("job_qualifications"),
    concat_ws(" ", col("job_highlights.Responsibilities")).alias("job_responsibilities")
)

# Write directly to S3 in Parquet format
#df_final.write.mode("append").parquet(args['output_path'])

# Write directly to S3 in CSV format
df_final.write.format("csv").option("header", "true").option("quote", None).mode("append").save(args['output_path'])
                    

# Commit job
job.commit()

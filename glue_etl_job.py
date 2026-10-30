import sys

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, trim

args = getResolvedOptions(
sys.argv,
[“JOB_NAME”, “INPUT_PATH”, “OUTPUT_PATH”]
)

sc = SparkContext.getOrCreate()
glueContext = GlueContext(sc)
spark = glueContext.spark_session

job = Job(glueContext)
job.init(args[“JOB_NAME”], args)

Extract: Read the source CSV file from Amazon S3

df = (
spark.read
.option(“header”, “true”)
.option(“inferSchema”, “true”)
.csv(args[“INPUT_PATH”])
)

print(“Original record count:”, df.count())

Transform: Remove duplicate records

df = df.dropDuplicates()

Transform: Remove records without a student ID

df = df.filter(
col(“student_id”).isNotNull()
)

Transform: Clean whitespace from text columns

df = df.withColumn(“name”, trim(col(“name”)))
df = df.withColumn(“department”, trim(col(“department”)))

Transform: Replace missing names with ‘Unknown’

df = df.fillna({“name”: “Unknown”})

Transform: Replace missing marks with 0

df = df.fillna({“marks”: 0})

print(“Cleaned record count:”, df.count())

Load: Save the cleaned data to Amazon S3

(
df.write
.mode(“overwrite”)
.option(“header”, “true”)
.csv(args[“OUTPUT_PATH”])
)

print(“ETL job completed successfully.”)

job.commit()
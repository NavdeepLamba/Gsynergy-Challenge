from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct

# Initialize Spark session with BigQuery and GCS support
spark = SparkSession.builder \
    .appName("GCS to BQ ETL - Uniqueness Check") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

bucket='gsynergy_challenge'
spark.conf.set('temporaryGcsBucket', bucket)

# Define GCS File Path (Replace with your actual GCS path)
GCS_FILE_PATH = "gs://gsynergy_challenge/hier.clnd.dlm.gz"

# Read GCS file into DataFrame (Assuming pipe-delimited gzipped file)
df = spark.read \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(GCS_FILE_PATH)

# Define Primary Key Column
PRIMARY_KEY = "fscldt_id"  # Change this to your actual PK column

# Check uniqueness by comparing total and distinct counts
uniqueness_check = df.agg(
    count(col(PRIMARY_KEY)).alias("total_count"),
    countDistinct(col(PRIMARY_KEY)).alias("distinct_count")
)

# Define BigQuery destination table
PROJECT_ID = "linear-sight-452704-a1"
DATASET_ID = "anjan_data_eng_learning"
TABLE_ID = "primary_key_uniqueness"
BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

# Write uniqueness results to BigQuery
uniqueness_check.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .save()

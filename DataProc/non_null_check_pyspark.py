from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, count

# Initialize Spark session with BigQuery and GCS support
spark = SparkSession.builder \
    .appName("GCS to BQ ETL") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

bucket='gsynergy_challenge'
spark.conf.set('temporaryGcsBucket', bucket)

# Define GCS File Path (Replace with your actual GCS path)
GCS_FILE_PATH = "gs://gsynergy_challenge/fact.averagecosts.dlm.gz"

# Read GCS file into DataFrame (Assuming pipe-delimited gzipped file)
df = spark.read \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(GCS_FILE_PATH)

# List of required columns for non-null check
required_columns = ["fscldt_id", "average_unit_standardcost", "average_unit_landedcost"]

# Perform Non-Null Check
non_null_counts = df.agg(
    *[count(when(col(c).isNotNull(), c)).alias(f"{c}_non_null_count") for c in required_columns]
)

# Define BigQuery destination table
PROJECT_ID = "linear-sight-452704-a1"
DATASET_ID = "anjan_data_eng_learning"
TABLE_ID = "non_null_counts_table"
BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

# Write DataFrame to BigQuery with createDisposition
non_null_counts.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .save()

spark.stop()

from pyspark.sql import SparkSession
from pyspark.sql.functions import lit

# Initialize Spark session with BigQuery and GCS support
spark = SparkSession.builder \
    .appName("GCS to BQ ETL - Data Type Validation") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='gsynergy_challenge'
spark.conf.set('temporaryGcsBucket', bucket)


# Define GCS File Path (Replace with your actual GCS path)
GCS_FILE_PATH = "gs://gsynergy_challenge/fact.averagecosts.dlm.gz"

# Read GCS file into DataFrame (Assuming pipe-delimited gzipped file)
df = spark.read \
    .option("header", "true") \
    .option("delimiter", "|") \
    .csv(GCS_FILE_PATH)

# Define expected data types (Modify as per your schema)
expected_schema = {
    "fscldt_id": "integer",
    "sku_id": "string",
    "average_unit_standardcost": "double"
}

# Function to get actual data types
actual_schema = {col_name: dtype for col_name, dtype in df.dtypes}

# Compare expected vs actual schema
validation_results = []
for column, expected_type in expected_schema.items():
    actual_type = actual_schema.get(column, "MISSING")
    is_valid = actual_type == expected_type
    validation_results.append((column, expected_type, actual_type, is_valid))

# Convert results into DataFrame
validation_df = spark.createDataFrame(validation_results, ["column_name", "expected_type", "actual_type", "is_valid"])

# Define BigQuery destination table
PROJECT_ID = "linear-sight-452704-a1"
DATASET_ID = "anjan_data_eng_learning"
TABLE_ID = "data_type_validation"
BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

# Write validation results to BigQuery
validation_df.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .save()



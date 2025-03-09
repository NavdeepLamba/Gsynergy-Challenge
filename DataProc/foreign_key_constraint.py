from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Foreign Key Constraint Validation") \
    .config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar") \
    .getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='gsynergy_challenge'
spark.conf.set('temporaryGcsBucket', bucket)

# Define GCS File Paths for fact and dimension tables
FACT_TABLE_PATH = "gs://gsynergy_challenge/fact.averagecosts.dlm.gz"
DIM_TABLE_PATH = "gs://gsynergy_challenge/hier.clnd.dlm.gz"

# Read fact and dimension tables from GCS (Assuming pipe-delimited gzipped files)
fact_df = spark.read.option("header", "true").option("delimiter", "|").csv(FACT_TABLE_PATH)
dim_df = spark.read.option("header", "true").option("delimiter", "|").csv(DIM_TABLE_PATH)

# Define Foreign Key Column and Primary Key Column
FACT_FK_COLUMN = "fscldt_id"   # Foreign Key in fact table
DIM_PK_COLUMN = "fscldt_id"    # Primary Key in dimension table

# Check for foreign key violations (fact records with missing dimension references)
invalid_fk_df = fact_df.join(dim_df, fact_df[FACT_FK_COLUMN] == dim_df[DIM_PK_COLUMN], "left_anti")

# Count total missing foreign key references
fk_violation_count = invalid_fk_df.agg(count("*").alias("missing_foreign_keys"))

# Define BigQuery destination table
PROJECT_ID = "linear-sight-452704-a1"
DATASET_ID = "anjan_data_eng_learning"
TABLE_ID = "fk_validation_results"
BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

# Write foreign key validation results to BigQuery
fk_violation_count.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .mode("overwrite") \
    .save()

print(f"Foreign Key validation results successfully written to {BQ_TABLE}")

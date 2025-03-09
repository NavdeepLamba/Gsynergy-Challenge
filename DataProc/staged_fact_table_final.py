from pyspark.sql import SparkSession


# Initialize Spark Session
spark = SparkSession.builder.appName("RetailDataPipeline").config("spark.jars", "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar").getOrCreate()

# Define temporary GCS bucket for Dataproc to write it's process data
bucket='gsynergy_challenge'
spark.conf.set('temporaryGcsBucket', bucket)

# Load and Normalize Hierarchy Tables
rtlloc_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/hier.rtlloc.dlm.gz", inferSchema=True) \
    .selectExpr("_c0 as rtlloc_id", "_c1 as store_name", "_c5 as region")

first_row = rtlloc_df.limit(1)  # Get the first row
rtlloc_df = rtlloc_df.subtract(first_row)

prod_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/hier.prod.dlm.gz", inferSchema=True) \
    .selectExpr("_c0 as sku_id", "_c9 as category", "_c7 as subcategory", "_c5 as brand")

first_row = prod_df.limit(1)  # Get the first row
prod_df = prod_df.subtract(first_row)

pricestate_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/hier.pricestate.dlm.gz", inferSchema=True) \
    .selectExpr("_c0 as price_substate_id", "_c3 as price_zone")

first_row = pricestate_df.limit(1)  # Get the first row
pricestate_df = pricestate_df.subtract(first_row)


possite_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/hier.possite.dlm.gz", inferSchema=True) \
    .selectExpr("_c0 as pos_site_id", "_c1 as store_name")

first_row = possite_df.limit(1)  # Get the first row
possite_df = possite_df.subtract(first_row)

clnd_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/hier.clnd.dlm.gz", inferSchema=True) \
    .selectExpr("_c0 as fscldt_id", "_c2 as fsclwk_id", "_c8 as fsclyr_id")
first_row = clnd_df.limit(1)  # Get the first row
clnd_df = clnd_df.subtract(first_row)

# Load Fact Tables
transactions_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/fact.transactions.dlm.gz",inferSchema=True) \
    .selectExpr("_c2 as type","_c4 as pos_site_id", "_c5 as sku_id", "_c6 as fscldt_id", "_c7 as price_substate_id","_c8 as sales_units", "_c9 as sales_dollars", "_c10 as discount_dollars")

first_row = transactions_df.limit(1)  # Get the first row
transactions_df = transactions_df.subtract(first_row)


average_costs_df = spark.read.option("delimiter", "|").csv("gs://gsynergy_challenge/fact.averagecosts.dlm.gz", inferSchema=True) \
    .toDF("_c0","sku_id", "avg_cost","_c2").select('sku_id',"avg_cost")
first_row = average_costs_df.limit(1)  # Get the first row
average_costs_df = average_costs_df.subtract(first_row)

# Establish Foreign Key Relationships in Staged Fact Table
staged_facts = transactions_df \
    .join(possite_df, "pos_site_id", "left") \
    .join(prod_df, "sku_id", "left") \
    .join(pricestate_df, "price_substate_id", "left") \
    .join(clnd_df, "fscldt_id", "left")



# Define BigQuery destination table
PROJECT_ID = "linear-sight-452704-a1"
DATASET_ID = "anjan_data_eng_learning"
TABLE_ID = "staged_facts"
BQ_TABLE = f"{PROJECT_ID}:{DATASET_ID}.{TABLE_ID}"

# Write DataFrame to BigQuery with createDisposition
staged_facts.write \
    .format("bigquery") \
    .option("table", BQ_TABLE) \
    .option("createDisposition", "CREATE_IF_NEEDED") \
    .save()

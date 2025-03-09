
# GSynergy-Challenge

## Overview
This repository contains the data engineering pipeline for processing retail transaction data using PySpark and BigQuery. The project includes schema inference, data validation, transformation, and aggregation to generate a refined materialized view for weekly sales.

## Project Structure

📂 GSynergy-Challenge │── 📂 BigQuery │ ├── mview_weekly_sales.md │── 📂 DataProc │ ├── staged_fact_table_final.py │── 📄 ER Diagram Summary.pdf │── 📄 ER Diagram _ GSynergy Challenge.pdf │── 📄 Pyspark Code _ Schema Inference Tool.pdf │── 📄 README.md


## Prerequisites
Ensure you have the following before running the pipeline:
- Google Cloud SDK installed and authenticated (`gcloud auth application-default login`)
- Google Cloud Storage (GCS) bucket with data files
- Google BigQuery dataset created
- Apache Spark installed (`pyspark` environment)

## Running the Pipeline
1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/Gsynergy-Challenge.git
   cd Gsynergy-Challenge

2. Run PySpark Data Processing

* Open Google Colab or a local Jupyter Notebook.
Mount your GCS bucket:

from google.colab import auth
auth.authenticate_user()
!gcloud auth application-default login

* Run the PySpark script:

spark-submit DataProc/staged_fact_table_final.py

3. Validate BigQuery Table

After processing, validate the mview_weekly_sales table in BigQuery :

SELECT * FROM `your_project.your_dataset.mview_weekly_sales` LIMIT 10;

* Verify the aggregated values by running:

  SELECT pos_site_id, sku_id, fsclwk_id, SUM(total_sales_units) 
FROM `your_project.your_dataset.mview_weekly_sales`
GROUP BY pos_site_id, sku_id, fsclwk_id;

4. Validation Checklist
Ensure the staged_facts table is successfully created with joined hierarchy data.
Check that mview_weekly_sales aggregates sales_units, sales_dollars, and discount_dollars.
Compare a sample transaction record with the corresponding aggregated weekly total.




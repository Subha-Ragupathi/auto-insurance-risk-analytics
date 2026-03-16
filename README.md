# Auto Insurance Risk Analytics Pipeline

## Overview
This project implements an end-to-end auto insurance risk analytics pipeline using a Databricks-based medallion architecture. It processes raw insurance policy data from a Kaggle dataset through bronze, silver, and gold layers to generate analytics-ready outputs for claim-rate reporting across customer, vehicle, and policy attributes.

## Business Problem
Insurance organizations need to understand how customer demographics, vehicle characteristics, safety features, and regional factors influence claim occurrence. Raw policy-level data is not directly suitable for analytics because it may contain duplicate records, inconsistent text values, missing values, and unstructured feature fields that require standardization.

## Solution
This project builds a layered data engineering pipeline that:

- ingests a raw Kaggle insurance CSV dataset
- stores source-aligned records in a bronze layer
- cleans and standardizes data in a silver layer
- derives business-friendly age bands and analytical dimensions
- creates gold-layer summary datasets for claim-rate analysis
- supports orchestration through a Databricks master pipeline notebook

## Business Value
The gold-layer outputs help insurance analysts and business teams evaluate claim trends across region, vehicle type, fuel type, age bands, and safety-related vehicle attributes. These outputs can support risk segmentation, operational analysis, portfolio monitoring, and downstream dashboarding.

## Tech Stack
- Python
- PySpark
- Delta Lake
- SQL
- Databricks
- GitHub
- GitHub Actions
- Kaggle public dataset

## End-to-End Flow
Raw Kaggle CSV  
→ Bronze Delta  
→ Silver Delta  
→ Gold Delta  
→ SQL Analytics / Reporting

## Architecture
![Architecture Diagram](docs/project_architecture.png)

## Repository Structure
- `data/raw` - source Kaggle CSV file
- `notebooks` - bronze, silver, gold, data-quality, and orchestration notebooks
- `sql` - analytical SQL queries on gold outputs
- `docs` - architecture, execution proof, and supporting screenshots
- `.github/workflows` - CI workflow for Python code checks

## Key Files
- `notebooks/01_bronze_ingestion.py`
- `notebooks/02_silver_transformation.py`
- `notebooks/03_gold_aggregation.py`
- `notebooks/04_data_quality_checks.py`
- `notebooks/05_master_pipeline`
- `sql/gold_queries.sql`
- `docs/project_architecture.png`

## Source Data
This project uses a Kaggle auto insurance dataset with fields such as:

- `policy_id`
- `subscription_length`
- `vehicle_age`
- `customer_age`
- `region_code`
- `region_density`
- `model`
- `fuel_type`
- `engine_type`
- `segment`
- `ncap_rating`
- `claim_status`

The dataset also includes multiple vehicle feature and safety-related attributes used for downstream risk analysis.

## Processing Layers

### Bronze Layer
Reads the raw Kaggle CSV dataset and stores it in Delta format with minimal transformation.

### Silver Layer
Applies cleansing and standardization rules such as:
- duplicate removal using `policy_id`
- trimming text fields
- uppercasing categorical fields
- null handling for `claim_status`
- standardization of selected vehicle and policy attributes

### Gold Layer
Builds business-ready summary outputs for:
- claim rate by region
- claim rate by segment
- claim rate by fuel type
- claim rate by vehicle age band
- claim rate by customer age band
- claim rate by NCAP rating

## Data Quality Checks
The project includes validation logic to identify:
- null values across all columns
- duplicate policy IDs

## Databricks Execution
The notebooks were executed successfully in Databricks using raw, bronze, silver, and gold Delta paths.

![Bronze Run](docs/databricks_bronze_success.png)
![Silver Run](docs/databricks_silver_success.png)
![Gold Run](docs/databricks_gold_success.png)
![DQ Run](docs/databricks_dq_success.png)

## Pipeline Orchestration
The end-to-end workflow is orchestrated through a Databricks master pipeline notebook that executes:

1. bronze ingestion  
2. silver transformation  
3. gold aggregation  
4. data-quality checks  

![Master Pipeline Success](docs/databricks_master_pipeline_success.png)
![Job Run Success](docs/databricks_job_success.png)

## Gold Layer Output Preview
![Gold Output Preview](docs/databricks_gold_preview.png)

## Sample Business Outputs
- claim rate by region
- claim rate by segment
- claim rate by fuel type
- claim rate by vehicle age band
- claim rate by customer age band
- claim rate by NCAP rating

## SQL Query Output Preview
![SQL Output Preview](docs/sql_claim_rate_preview.png)

## How to Run
1. Upload the Kaggle insurance CSV to the Databricks raw volume
2. Run `01_bronze_ingestion.py`
3. Run `02_silver_transformation.py`
4. Run `03_gold_aggregation.py`
5. Run `04_data_quality_checks.py`
6. Run `05_master_pipeline` for end-to-end orchestration

## Future Enhancements
- parameterize notebook inputs and output paths
- extend monitoring and job-level retry handling
- add dashboard screenshots for business consumption

## Status
Core pipeline implementation completed with Databricks execution, gold outputs, and notebook orchestration.

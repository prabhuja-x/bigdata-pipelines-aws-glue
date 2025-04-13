# E-commerce Transaction ETL Pipeline

This project implements an ETL (Extract, Transform, Load) pipeline for processing e-commerce transaction data using AWS services.

## Purpose

The pipeline aims to clean, transform, and analyze raw e-commerce transaction data (e.g., from CSV/JSON files) to identify sales trends and prepare the data for analytics.

## Tech Stack

*   **Data Storage (Raw):** AWS S3
*   **Transformation:** AWS Glue or AWS EMR with PySpark
*   **Analytics:** AWS Redshift or AWS Athena
*   **Orchestration:** AWS Step Functions
*   **Language:** Python (using Boto3 and PySpark)

## Project Structure

*   `s3_setup.py`: Script for setting up S3 buckets and handling S3 interactions (e.g., uploading sample data).
*   `glue_job.py`: PySpark script designed to run as an AWS Glue job. Performs transformations like currency conversion, category enrichment, and time-based aggregations.
*   `redshift_setup.py`: Script containing logic to set up analytics tables in AWS Athena (via Glue Data Catalog) or AWS Redshift (commented out). Includes functions to create databases/tables and load data.
*   `step_functions_definition.json`: Amazon States Language (ASL) definition for the AWS Step Functions state machine that orchestrates the ETL workflow, starting with the Glue job.
*   `README.md`: This file.

## Setup and Usage (High-Level)

1.  **Configure AWS Credentials:** Ensure your environment is configured with appropriate AWS credentials (e.g., via environment variables, IAM role).
2.  **Install Dependencies:** `pip install boto3 pyspark` (Note: PySpark is usually provided by the Glue/EMR environment).
3.  **Customize Configuration:**
    *   Update bucket names, paths, Glue job names, Redshift/Athena details, and IAM roles within the Python scripts (`s3_setup.py`, `glue_job.py`, `redshift_setup.py`) and the Step Functions definition (`step_functions_definition.json`).
    *   Provide actual reference data (currency rates, product categories) in S3.
4.  **Deploy AWS Resources:**
    *   Run `s3_setup.py` (or manually create the S3 bucket structure).
    *   Create an AWS Glue job using `glue_job.py`.
    *   Choose between Athena or Redshift:
        *   **Athena:** Run `redshift_setup.py` to create the Glue database and table.
        *   **Redshift:** Uncomment and configure the Redshift sections in `redshift_setup.py`, create the cluster and IAM role, then run the script.
    *   Create an AWS Step Functions state machine using `step_functions_definition.json`.
5.  **Run the Pipeline:** Trigger the Step Functions state machine with the required input parameters (S3 paths, etc.).

import boto3
import logging
import os
import time
import json

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_TRANSFORMED_DATA_PATH = os.getenv('S3_TRANSFORMED_DATA_PATH', 's3://your-ecommerce-data-bucket/transformed/') # Base path
GLUE_DATABASE_NAME = os.getenv('GLUE_DATABASE_NAME', 'ecommerce_db')
GLUE_TABLE_NAME = os.getenv('GLUE_TABLE_NAME', 'transactions_transformed')
ATHENA_QUERY_OUTPUT_LOCATION = os.getenv('ATHENA_QUERY_OUTPUT_LOCATION', 's3://your-athena-query-results-bucket/output/') # Needs separate bucket

# Redshift specific config (Uncomment and configure if using Redshift)
# REDSHIFT_CLUSTER_ID = os.getenv('REDSHIFT_CLUSTER_ID', 'your-redshift-cluster')
# REDSHIFT_DATABASE_NAME = os.getenv('REDSHIFT_DATABASE_NAME', 'dev')
# REDSHIFT_USER = os.getenv('REDSHIFT_USER', 'awsuser') # Or your DB user
# REDSHIFT_IAM_ROLE_ARN = os.getenv('REDSHIFT_IAM_ROLE_ARN', 'arn:aws:iam::ACCOUNT_ID:role/YourRedshiftS3AccessRole') # Role for Redshift to access S3
# REDSHIFT_TABLE_NAME = 'transactions_transformed'

# Initialize AWS clients
glue_client = boto3.client('glue', region_name=AWS_REGION)
athena_client = boto3.client('athena', region_name=AWS_REGION)
# redshift_data_client = boto3.client('redshift-data', region_name=AWS_REGION) # Uncomment if using Redshift

# --- Athena Setup Functions ---

def create_glue_database(database_name):
    """Creates a Glue database if it doesn't exist."""
    try:
        glue_client.get_database(Name=database_name)
        logging.info(f"Glue database '{database_name}' already exists.")
    except glue_client.exceptions.EntityNotFoundException:
        logging.info(f"Creating Glue database '{database_name}'...")
        try:
            glue_client.create_database(DatabaseInput={'Name': database_name})
            logging.info(f"Glue database '{database_name}' created successfully.")
        except ClientError as e:
            logging.error(f"Failed to create Glue database '{database_name}': {e}")
            raise
    except ClientError as e:
        logging.error(f"Error checking Glue database '{database_name}': {e}")
        raise

def create_or_update_glue_table(database_name, table_name, s3_location, columns, partition_keys):
    """Creates or updates a Glue table pointing to S3 data (for Athena)."""
    table_input = {
        'Name': table_name,
        'StorageDescriptor': {
            'Columns': columns,
            'Location': s3_location,
            'InputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat',
            'OutputFormat': 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat',
            'SerdeInfo': {
                'SerializationLibrary': 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe',
                'Parameters': {'serialization.format': '1'}
            },
            'Compressed': False, # Parquet handles compression internally
            'NumberOfBuckets': -1,
            'StoredAsSubDirectories': False
        },
        'PartitionKeys': partition_keys,
        'TableType': 'EXTERNAL_TABLE',
        'Parameters': {
            'classification': 'parquet', # Important for Glue Crawlers and Athena
            'projection.enabled': 'true', # Enable partition projection if applicable
            # Add projection definitions if using projection
            # 'projection.load_date.type': 'date',
            # 'projection.load_date.format': 'yyyy-MM-dd',
            # 'projection.load_date.range': '2024-01-01,NOW', # Adjust range
            # 'storage.location.template': f'{s3_location}load_date=${{load_date}}'
        }
    }

    try:
        # Try updating first
        glue_client.update_table(DatabaseName=database_name, TableInput=table_input)
        logging.info(f"Glue table '{database_name}.{table_name}' updated successfully.")
    except glue_client.exceptions.EntityNotFoundException:
        # If it doesn't exist, create it
        logging.info(f"Glue table '{database_name}.{table_name}' not found. Creating...")
        try:
            glue_client.create_table(DatabaseName=database_name, TableInput=table_input)
            logging.info(f"Glue table '{database_name}.{table_name}' created successfully.")
        except ClientError as e:
            logging.error(f"Failed to create Glue table '{database_name}.{table_name}': {e}")
            raise
    except ClientError as e:
        logging.error(f"Failed to update Glue table '{database_name}.{table_name}': {e}")
        raise

def repair_table_partitions(database_name, table_name):
    """Runs MSCK REPAIR TABLE in Athena to discover new partitions."""
    logging.info(f"Running MSCK REPAIR TABLE for '{database_name}.{table_name}'...")
    query = f"MSCK REPAIR TABLE `{database_name}`.`{table_name}`;"
    try:
        response = athena_client.start_query_execution(
            QueryString=query,
            QueryExecutionContext={'Database': database_name},
            ResultConfiguration={'OutputLocation': ATHENA_QUERY_OUTPUT_LOCATION}
        )
        query_execution_id = response['QueryExecutionId']
        logging.info(f"Started MSCK REPAIR TABLE query execution: {query_execution_id}")

        # Wait for query completion
        while True:
            stats = athena_client.get_query_execution(QueryExecutionId=query_execution_id)
            status = stats['QueryExecution']['Status']['State']
            if status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                logging.info(f"MSCK REPAIR TABLE query finished with status: {status}")
                if status != 'SUCCEEDED':
                     logging.error(f"MSCK REPAIR TABLE failed: {stats['QueryExecution']['Status'].get('StateChangeReason', 'No reason provided')}")
                break
            time.sleep(5)
        return status == 'SUCCEEDED'
    except ClientError as e:
        logging.error(f"Failed to execute MSCK REPAIR TABLE: {e}")
        return False

# --- Redshift Setup Functions (Uncomment and implement if using Redshift) ---

# def execute_redshift_statement(sql_statement):
#     """Executes a SQL statement on the Redshift cluster."""
#     logging.info(f"Executing Redshift statement: {sql_statement[:100]}...") # Log truncated statement
#     try:
#         response = redshift_data_client.execute_statement(
#             ClusterIdentifier=REDSHIFT_CLUSTER_ID,
#             Database=REDSHIFT_DATABASE_NAME,
#             DbUser=REDSHIFT_USER,
#             Sql=sql_statement
#         )
#         query_id = response['Id']
#         logging.info(f"Started Redshift query execution: {query_id}")

#         # Wait for completion
#         while True:
#             desc = redshift_data_client.describe_statement(Id=query_id)
#             status = desc['Status']
#             if status in ['FINISHED', 'FAILED', 'ABORTED']:
#                 logging.info(f"Redshift query finished with status: {status}")
#                 if status != 'FINISHED':
#                     logging.error(f"Redshift query failed: {desc.get('Error', 'No error details')}")
#                 return status == 'FINISHED'
#             time.sleep(5)
#     except ClientError as e:
#         logging.error(f"Failed to execute Redshift statement: {e}")
#         return False

# def create_redshift_table():
#     """Creates the target table in Redshift if it doesn't exist."""
#     # Note: Adjust data types based on Redshift specifics
#     create_table_sql = f"""
#     CREATE TABLE IF NOT EXISTS {REDSHIFT_TABLE_NAME} (
#         transaction_id VARCHAR(256),
#         user_id VARCHAR(256),
#         product_id VARCHAR(256),
#         category VARCHAR(256),
#         amount DECIMAL(18, 2),
#         currency VARCHAR(10),
#         amount_usd DECIMAL(18, 2),
#         "timestamp" TIMESTAMP, -- Use quotes if 'timestamp' is reserved
#         transaction_date DATE,
#         transaction_year INTEGER,
#         transaction_month INTEGER,
#         transaction_week INTEGER,
#         transaction_day INTEGER,
#         load_date DATE -- Add load date if needed
#     );
#     """
#     return execute_redshift_statement(create_table_sql)

# def copy_data_from_s3_to_redshift(s3_path):
#     """Copies data from S3 (Parquet) to the Redshift table."""
#     # Ensure the S3 path ends with a '/' if it's a prefix
#     if not s3_path.endswith('/'):
#         s3_path += '/'

#     copy_sql = f"""
#     COPY {REDSHIFT_TABLE_NAME}
#     FROM '{s3_path}'
#     IAM_ROLE '{REDSHIFT_IAM_ROLE_ARN}'
#     FORMAT AS PARQUET;
#     """
#     # Add options like MANIFEST, REGION, etc. if needed
#     # Consider TRUNCATECOLUMNS, MAXERROR, etc. for error handling

#     # Optional: Staging table approach for idempotent loads
#     # 1. Create staging table (like target but temporary)
#     # 2. COPY into staging table
#     # 3. DELETE from target where exists in staging (based on key)
#     # 4. INSERT into target from staging

#     return execute_redshift_statement(copy_sql)


# --- Main Execution Logic ---
if __name__ == "__main__":
    logging.info("Starting Redshift/Athena setup...")

    # --- Option 1: Setup for Athena ---
    logging.info("Setting up for AWS Athena...")
    try:
        # 1. Ensure Glue Database exists
        create_glue_database(GLUE_DATABASE_NAME)

        # 2. Define Glue Table Schema (matches PySpark output)
        glue_columns = [
            {'Name': 'transaction_id', 'Type': 'string'},
            {'Name': 'user_id', 'Type': 'string'},
            {'Name': 'product_id', 'Type': 'string'},
            {'Name': 'category', 'Type': 'string'},
            {'Name': 'amount', 'Type': 'float'},
            {'Name': 'currency', 'Type': 'string'},
            {'Name': 'amount_usd', 'Type': 'float'},
            {'Name': 'timestamp', 'Type': 'timestamp'},
            {'Name': 'transaction_date', 'Type': 'date'},
            {'Name': 'transaction_year', 'Type': 'int'},
            {'Name': 'transaction_month', 'Type': 'int'},
            {'Name': 'transaction_week', 'Type': 'int'},
            {'Name': 'transaction_day', 'Type': 'int'}
        ]
        # Define partition keys based on how data is written in S3 (e.g., by load_date)
        glue_partition_keys = [
            {'Name': 'load_date', 'Type': 'date'}
        ]

        # 3. Create or Update Glue Table
        create_or_update_glue_table(
            GLUE_DATABASE_NAME,
            GLUE_TABLE_NAME,
            S3_TRANSFORMED_DATA_PATH, # Base path, partitions are discovered
            glue_columns,
            glue_partition_keys
        )

        # 4. Discover Partitions (if not using partition projection)
        # If using partition projection configured in create_or_update_glue_table,
        # MSCK REPAIR is often not needed.
        # repair_table_partitions(GLUE_DATABASE_NAME, GLUE_TABLE_NAME)
        logging.info("Athena setup completed. Table should be queryable.")

    except Exception as e:
        logging.error(f"Athena setup failed: {e}")
        exit(1)

    # --- Option 2: Setup for Redshift (Uncomment to use) ---
    # logging.info("Setting up for AWS Redshift...")
    # try:
    #     # 1. Create Redshift Table (Idempotent)
    #     if not create_redshift_table():
    #         raise Exception("Failed to create Redshift table.")

    #     # 2. Copy data from the latest S3 partition
    #     #    You'll need to determine the specific S3 path for the latest load.
    #     #    This might come from Step Functions context or by listing S3 prefixes.
    #     #    Example: Assuming the latest load date is known
    #     latest_load_date = datetime.date.today().isoformat() # Or get from context
    #     latest_s3_partition_path = os.path.join(S3_TRANSFORMED_DATA_PATH, f"load_date={latest_load_date}/")
    #     logging.info(f"Copying data from {latest_s3_partition_path} to Redshift...")

    #     if not copy_data_from_s3_to_redshift(latest_s3_partition_path):
    #          raise Exception("Failed to copy data from S3 to Redshift.")

    #     logging.info("Redshift setup and data load completed.")

    # except Exception as e:
    #     logging.error(f"Redshift setup failed: {e}")
    #     exit(1)

    logging.info("Redshift/Athena setup script finished.")

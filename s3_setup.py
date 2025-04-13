import boto3
import logging
import os
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Load environment variables or use defaults
AWS_REGION = os.getenv('AWS_REGION', 'us-east-1')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'your-ecommerce-data-bucket') # Replace with your desired bucket name
RAW_DATA_PREFIX = 'raw/'
TRANSFORMED_DATA_PREFIX = 'transformed/'

# Initialize S3 client
s3_client = boto3.client('s3', region_name=AWS_REGION)
s3_resource = boto3.resource('s3', region_name=AWS_REGION)

def create_s3_bucket(bucket_name, region=None):
    """
    Create an S3 bucket in a specified region.

    If a region is not specified, the bucket is created in the S3 default
    region (us-east-1).

    :param bucket_name: Bucket to create
    :param region: String region to create bucket in, e.g., 'us-west-2'
    :return: True if bucket created, else False
    """
    try:
        if region is None:
            s3_client.create_bucket(Bucket=bucket_name)
            logging.info(f"Bucket '{bucket_name}' created successfully in default region (us-east-1).")
        else:
            location = {'LocationConstraint': region}
            s3_client.create_bucket(Bucket=bucket_name,
                                    CreateBucketConfiguration=location)
            logging.info(f"Bucket '{bucket_name}' created successfully in region '{region}'.")
        return True
    except ClientError as e:
        if e.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            logging.warning(f"Bucket '{bucket_name}' already exists and is owned by you.")
            return True
        elif e.response['Error']['Code'] == 'BucketAlreadyExists':
            logging.error(f"Bucket '{bucket_name}' already exists and is owned by someone else.")
            return False
        else:
            logging.error(f"Failed to create bucket '{bucket_name}': {e}")
            return False

def upload_file_to_s3(file_path, bucket_name, object_key):
    """
    Upload a file to an S3 bucket.

    :param file_path: Path to the file to upload
    :param bucket_name: Bucket to upload to
    :param object_key: S3 object key (path within the bucket)
    :return: True if file uploaded, else False
    """
    try:
        s3_client.upload_file(file_path, bucket_name, object_key)
        logging.info(f"File '{file_path}' uploaded successfully to 's3://{bucket_name}/{object_key}'.")
        return True
    except ClientError as e:
        logging.error(f"Failed to upload file '{file_path}' to S3: {e}")
        return False
    except FileNotFoundError:
        logging.error(f"The file '{file_path}' was not found.")
        return False

def check_bucket_exists(bucket_name):
    """Check if an S3 bucket exists."""
    try:
        s3_resource.meta.client.head_bucket(Bucket=bucket_name)
        logging.info(f"Bucket '{bucket_name}' exists.")
        return True
    except ClientError as e:
        error_code = int(e.response['Error']['ResponseMetadata']['HTTPStatusCode'])
        if error_code == 404:
            logging.warning(f"Bucket '{bucket_name}' does not exist.")
            return False
        else:
            logging.error(f"Error checking bucket '{bucket_name}': {e}")
            return False

if __name__ == "__main__":
    logging.info("Starting S3 setup...")

    # 1. Check if the bucket exists, create if not
    if not check_bucket_exists(S3_BUCKET_NAME):
        if create_s3_bucket(S3_BUCKET_NAME, region=AWS_REGION):
             logging.info(f"Bucket '{S3_BUCKET_NAME}' is ready.")
        else:
            logging.error(f"Exiting due to failure in creating bucket '{S3_BUCKET_NAME}'.")
            exit(1) # Exit if bucket creation fails
    else:
        logging.info(f"Bucket '{S3_BUCKET_NAME}' already exists.")

    # Example Usage: Upload a sample raw data file (replace with actual data)
    # Create a dummy raw data file for demonstration
    sample_raw_file = 'sample_raw_data.csv'
    with open(sample_raw_file, 'w') as f:
        f.write("transaction_id,user_id,product_id,amount,currency,timestamp\n")
        f.write("1,101,P100,50.00,USD,2025-04-12T10:00:00Z\n")
        f.write("2,102,P200,75.50,EUR,2025-04-12T11:30:00Z\n")

    raw_object_key = f"{RAW_DATA_PREFIX}{sample_raw_file}"
    if upload_file_to_s3(sample_raw_file, S3_BUCKET_NAME, raw_object_key):
        logging.info("Sample raw data uploaded.")
    else:
        logging.error("Failed to upload sample raw data.")

    # Clean up the dummy file
    os.remove(sample_raw_file)

    logging.info("S3 setup script finished.")

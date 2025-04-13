import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf, year, month, weekofyear, dayofmonth, to_timestamp, lit
from pyspark.sql.types import StringType, FloatType, StructType, StructField, TimestampType, IntegerType
import logging
import os
import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# --- Configuration ---
# Get job arguments (passed by Glue)
args = getResolvedOptions(sys.argv, ['JOB_NAME', 'S3_INPUT_PATH', 'S3_OUTPUT_PATH', 'CURRENCY_RATES_PATH', 'PRODUCT_CATEGORIES_PATH'])

S3_INPUT_PATH = args['S3_INPUT_PATH'] # e.g., s3://your-bucket/raw/
S3_OUTPUT_PATH = args['S3_OUTPUT_PATH'] # e.g., s3://your-bucket/transformed/
CURRENCY_RATES_PATH = args['CURRENCY_RATES_PATH'] # e.g., s3://your-bucket/reference/currency_rates.csv
PRODUCT_CATEGORIES_PATH = args['PRODUCT_CATEGORIES_PATH'] # e.g., s3://your-bucket/reference/product_categories.csv
TARGET_CURRENCY = 'USD' # Define the target currency for conversion

# --- Initialize Spark and Glue Contexts ---
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# --- Define Schemas (Adjust based on your actual data) ---
transaction_schema = StructType([
    StructField("transaction_id", StringType(), True),
    StructField("user_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("amount", FloatType(), True),
    StructField("currency", StringType(), True),
    StructField("timestamp", StringType(), True) # Assuming timestamp is initially a string
])

currency_rates_schema = StructType([
    StructField("currency", StringType(), True),
    StructField("rate_to_usd", FloatType(), True),
    StructField("rate_date", TimestampType(), True) # Assuming rates might change over time
])

product_categories_schema = StructType([
    StructField("product_id", StringType(), True),
    StructField("category", StringType(), True)
])


# --- Helper Functions ---
def get_latest_rate(currency, rates_df):
    """Placeholder function to get the latest conversion rate for a currency."""
    # In a real scenario, you'd join based on date or use a more sophisticated lookup
    rate_row = rates_df.filter(col("currency") == currency).orderBy(col("rate_date").desc()).first()
    return rate_row['rate_to_usd'] if rate_row else 1.0 # Default to 1.0 if rate not found

def convert_currency(amount, currency, rates_df):
    """Converts an amount from a given currency to the target currency (USD)."""
    if currency == TARGET_CURRENCY:
        return amount
    rate = get_latest_rate(currency, rates_df) # Simplified rate lookup
    return amount * rate if rate else amount # Handle missing rates

# Register UDF for currency conversion
convert_currency_udf = udf(lambda amount, currency: convert_currency(amount, currency, currency_rates_broadcast.value), FloatType())

# --- ETL Steps ---
try:
    # 1. Read Raw Transaction Data from S3
    logging.info(f"Reading raw transaction data from {S3_INPUT_PATH}")
    # Assuming CSV format, adjust options as needed (e.g., for JSON)
    raw_transactions_dyf = glueContext.create_dynamic_frame.from_options(
        connection_type="s3",
        connection_options={"paths": [S3_INPUT_PATH]},
        format="csv",
        format_options={"withHeader": True},
        transformation_ctx="raw_transactions_dyf"
    )
    raw_transactions_df = raw_transactions_dyf.toDF()
    # Apply schema and handle potential parsing errors if needed
    # For simplicity, assuming direct mapping works or use .cast()
    raw_transactions_df = raw_transactions_df.withColumn("timestamp", to_timestamp(col("timestamp"))) # Convert timestamp string to TimestampType
    raw_transactions_df = raw_transactions_df.withColumn("amount", col("amount").cast(FloatType())) # Ensure amount is float

    logging.info(f"Read {raw_transactions_df.count()} raw transactions.")
    raw_transactions_df.printSchema()
    raw_transactions_df.show(5, truncate=False)

    # 2. Read Reference Data (Currency Rates and Product Categories)
    logging.info(f"Reading currency rates from {CURRENCY_RATES_PATH}")
    currency_rates_df = spark.read.csv(CURRENCY_RATES_PATH, header=True, schema=currency_rates_schema)
    currency_rates_df = currency_rates_df.withColumn("rate_date", to_timestamp(col("rate_date"))) # Ensure date is TimestampType
    currency_rates_broadcast = sc.broadcast(currency_rates_df.collect()) # Broadcast for UDF efficiency
    logging.info("Currency rates loaded.")
    currency_rates_df.show(5)

    logging.info(f"Reading product categories from {PRODUCT_CATEGORIES_PATH}")
    product_categories_df = spark.read.csv(PRODUCT_CATEGORIES_PATH, header=True, schema=product_categories_schema)
    logging.info("Product categories loaded.")
    product_categories_df.show(5)

    # 3. Transformations
    logging.info("Starting transformations...")

    #   a. Currency Conversion
    transformed_df = raw_transactions_df.withColumn(
        f"amount_{TARGET_CURRENCY.lower()}",
        convert_currency_udf(col("amount"), col("currency"))
    )
    logging.info("Currency conversion applied.")
    transformed_df.show(5, truncate=False)

    #   b. Category Enrichment
    #      Use a left join to add category information based on product_id
    transformed_df = transformed_df.join(
        product_categories_df,
        transformed_df.product_id == product_categories_df.product_id,
        "left_outer"
    ).drop(product_categories_df.product_id) # Drop duplicate product_id column
    logging.info("Category enrichment applied.")
    transformed_df.show(5, truncate=False)

    #   c. Time-based Aggregations (Example: Add date/time components)
    transformed_df = transformed_df.withColumn("transaction_date", col("timestamp").cast("date"))
    transformed_df = transformed_df.withColumn("transaction_year", year(col("timestamp")))
    transformed_df = transformed_df.withColumn("transaction_month", month(col("timestamp")))
    transformed_df = transformed_df.withColumn("transaction_week", weekofyear(col("timestamp")))
    transformed_df = transformed_df.withColumn("transaction_day", dayofmonth(col("timestamp")))
    logging.info("Time components added.")
    transformed_df.show(5, truncate=False)

    # Select and potentially rename columns for the final output
    final_df = transformed_df.select(
        "transaction_id",
        "user_id",
        "product_id",
        "category",
        "amount",
        "currency",
        f"amount_{TARGET_CURRENCY.lower()}",
        "timestamp",
        "transaction_date",
        "transaction_year",
        "transaction_month",
        "transaction_week",
        "transaction_day"
    )
    logging.info("Final column selection done.")
    final_df.printSchema()

    # 4. Write Transformed Data to S3
    #    Partitioning by date is common for analytics efficiency
    output_path_with_date = os.path.join(S3_OUTPUT_PATH, f"load_date={datetime.date.today().isoformat()}")
    logging.info(f"Writing transformed data to {output_path_with_date}")

    # Convert back to DynamicFrame for writing with Glue's optimizations (optional but recommended)
    final_dyf = DynamicFrame.fromDF(final_df, glueContext, "final_dyf")

    # Write data in a query-friendly format like Parquet
    glueContext.write_dynamic_frame.from_options(
        frame=final_dyf,
        connection_type="s3",
        connection_options={"path": output_path_with_date},
        format="parquet",
        transformation_ctx="datasink"
    )
    logging.info("Transformed data successfully written to S3.")

except Exception as e:
    logging.error(f"An error occurred during the Glue job: {e}")
    # Potentially raise the error again or handle cleanup
    raise e
finally:
    # Commit the job - signals completion to Glue
    job.commit()
    logging.info("Glue job committed.")
    # Stop Spark context if running locally or in certain environments
    # sc.stop() # Usually managed by Glue itself

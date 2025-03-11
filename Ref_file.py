#Reference

import pandas as pd
import boto3
from io import StringIO
import logging
from datetime import datetime

# Set up logging to track pipeline execution
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuration
S3_BUCKET = 'my-data-engineering-bucket'  # Replace with your S3 bucket name
S3_OUTPUT_KEY = f'processed_data/processed_data_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv'
AWS_REGION = 'us-east-1'  # Replace with your AWS region

# Sample data source (could be an API, database, or file in real scenarios)
def extract_data():
    """Extract data from a source (simulated here with a dictionary)."""
    logger.info("Extracting data...")
    sample_data = {
        'id': [1, 2, 3, 4],
        'name': ['Alice', 'Bob', 'Charlie', 'David'],
        'sales': [100, 150, None, 200],
        'date': ['2025-01-01', '2025-02-01', '2025-03-01', '2025-04-01']
    }
    df = pd.DataFrame(sample_data)
    return df

# Data transformation
def transform_data(df):
    """Clean and transform the extracted data."""
    logger.info("Transforming data...")
    # Fill missing sales values with 0
    df['sales'] = df['sales'].fillna(0)
    # Convert date to datetime
    df['date'] = pd.to_datetime(df['date'])
    # Add a new column: sales_category
    df['sales_category'] = df['sales'].apply(lambda x: 'High' if x > 150 else 'Low')
    return df

# Load data to AWS S3
def load_to_s3(df, bucket, key, region):
    """Load the transformed data to an S3 bucket."""
    logger.info(f"Loading data to S3 bucket: {bucket}/{key}")
    # Initialize S3 client
    s3_client = boto3.client('s3', region_name=region)
    
    # Convert DataFrame to CSV string
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Upload to S3
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=csv_buffer.getvalue()
    )
    logger.info("Data successfully loaded to S3.")

# Main ETL pipeline
def run_etl_pipeline():
    """Run the complete ETL process."""
    try:
        # Extract
        raw_data = extract_data()
        logger.info(f"Extracted data:\n{raw_data}")
        
        # Transform
        transformed_data = transform_data(raw_data)
        logger.info(f"Transformed data:\n{transformed_data}")
        
        # Load to S3
        load_to_s3(transformed_data, S3_BUCKET, S3_OUTPUT_KEY, AWS_REGION)
        
    except Exception as e:
        logger.error(f"ETL pipeline failed: {str(e)}")
        raise

if __name__ == "__main__":
    logger.info("Starting ETL pipeline...")
    run_etl_pipeline()
    logger.info("ETL pipeline completed.")

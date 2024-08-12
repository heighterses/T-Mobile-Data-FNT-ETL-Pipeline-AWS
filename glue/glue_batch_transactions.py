"""
This Glue Job imports the batch file from Raw bucket and transforms before sending to stage
"""

# packages
import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
import boto3
import pandas as pd
from io import BytesIO
from datetime import datetime
import json

# Get command-line arguments
args = getResolvedOptions(sys.argv, 
                          ['JOB_NAME', 
                           'source_key', 
                           'source_bucket', 
                           'dest_bucket',
                           'batch_file_name',
                           'batch_timestamp'])
source_key = args['source_key']
source_bucket = args['source_bucket']
dest_bucket = args['dest_bucket']
batch_file_name = args['batch_file_name']
batch_timestamp = args['batch_timestamp']

# Initialize Spark context, Glue context, and the Glue job
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Initialize a session using Amazon S3
s3 = boto3.client('s3')

# Download the file from S3
response = s3.get_object(Bucket=source_bucket, Key=source_key)
file_content = response['Body'].read()

# Assign column headers based on known schema
column_headers_all = ['recordType','creditCardLastFour','surrogateAccountId','customerID','transactionEffectiveDate','transactionPostingDate','transactionPostingTime','authorizationDate','transactionCode','transactionType','transactionDescription', \
                  'dbCrIndicator','transactionReferenceNumber','internalExternalBrandFlag','transactionSequenceNumber','authorizationCode','transactionAmount','transferFlag','transactionCurrencyCode','billedCurrencyCode','conversionRate','merchantStore', \
                  'merchantID','merchantCategoryCode','merchantName','merchantCity','merchantState','merchantCountry','merchantPostalCode','invoiceNumber']

column_headers_drop = ['recordType']

# Assuming the .dat file is a CSV-like format, read it into a pandas DataFrame
# Adjust the parameters of pd.read_csv() as needed for your specific file format
# Read the file into a pandas DataFrame, skipping the first row and the last row
# Read everything as string, will change data types later
df = pd.read_csv(BytesIO(file_content), delimiter=',',skiprows=1, skipfooter=1, engine='python', on_bad_lines='skip', names = column_headers_all, dtype=str)
df = df.drop(column_headers_drop, axis=1)

# Define the desired data types for each column
dtype_dict = {
    'creditCardLastFour':str,
    'surrogateAccountId':str,
    'customerID':str,
    'transactionEffectiveDate':str,
    'transactionPostingDate':str,
    'transactionPostingTime':str,
    'authorizationDate':str,
    'transactionCode':int,
    'transactionType':int,
    'transactionDescription':str,
    'dbCrIndicator':str,
    'transactionReferenceNumber':str,
    'internalExternalBrandFlag':str,
    'transactionSequenceNumber':float,
    'authorizationCode':str,
    'transactionAmount':float,
    'transferFlag':str,
    'transactionCurrencyCode':str, #TODO double check schema
    'billedCurrencyCode':str, #TODO double check schema
    'conversionRate':float,
    'merchantStore':int,
    'merchantID':str,
    'merchantCategoryCode':str,
    'merchantName':str,
    'merchantCity':str,
    'merchantState':str,
    'merchantCountry':str,
    'merchantPostalCode':str,
    'invoiceNumber':str,
}
df = df.astype(dtype_dict)

# Format the current date and time as MM-DD-YYYY HH:MM:SS
now = datetime.now()
date_time_str1 = now.strftime("%m-%d-%Y %H:%M:%S")
date_time_str2 = now.strftime("%m-%d-%Y_%H:%M:%S")

# Add datetime to df
df.insert(loc=0, column='LastTimestampUpdated', value=date_time_str1)

# Add latestBatchTimestamp and latestBatchFileName to df
df.insert(loc=0, column='LatestBatchTimestamp', value=batch_timestamp)
df.insert(loc=0, column='LatestBatchFileName', value=batch_file_name)

# format as csv and save to s3
extension = ".csv"
s3_prefix = "s3://"
new_file_name = f"{s3_prefix}{dest_bucket}/cof-transactions/cof_staged_transactions_{date_time_str2}.{extension}"

# Convert Pandas DataFrame to PySpark DataFrame
spark_df = spark.createDataFrame(df)

# Write the dataframe to the specified S3 path in CSV format
spark_df.write\
     .format("csv")\
     .option("quote", None)\
     .option("header", "true")\
     .mode("append")\
     .save(new_file_name)

# Create json file with job details for subsequent Lambda functions
result = {}
result['batchType'] = 'Transactions'
result['batchFileName'] = batch_file_name
result['timestamp'] = date_time_str1
result['s3_bucket'] = dest_bucket
result['s3_key'] = f"cof-transactions/cof_staged_transactions_{date_time_str2}.{extension}"
result['my_key'] = f"cof-transactions/cof_staged_transactions_metadata.json"

# Write json file to S3
json_obj = json.dumps(result)
s3.put_object(Bucket=dest_bucket, Key=result['my_key'], Body=json_obj)

# Commit the Glue job
job.commit()
"""
This Lambda function validates batch file schema
"""

# packages
import json
import pandas as pd
#from cerberus import Validator
from datetime import datetime
import boto3 # type: ignore
import os
from io import BytesIO
import logging
import sys

# initiate logger for CloudWatch
logger = logging.getLogger()
logger.setLevel(logging.INFO)


###### logger function ######
def logger_function(message, type="info"):
    """
    Helper function for providing logger messages for CloudWatch

    Args:
        message (string): The message to display
        type (string): Either "info" or "error"
    """
    if type == 'info':
        logger.info(message)
    elif type == 'error':
        logger.error(message)

    return


###### keyword function ######
def contains_keywords(s):
    # Remove spaces and convert to lowercase
    s = s.replace(" ", "").lower()
    
    keywords = ["transaction", "account", "cardholder", "application"]
    
    # TODO update with correct glue job name
    if 'transaction' in s:
        return 'dev-fnt-0501651-glue-batch-transactions'
    elif 'account' in s:
        return 'dev-fnt-glue-batch-account-summary'
    elif 'cardholder' in s:
        return 'dev-fnt-0501651-glue-batch-cardholder'
    elif 'application' in s:
        return 'dev-fnt-0501651-glue-batch-application'
    else: 
        logger_function("ERROR: Invalid batch file name.", type="error")
        return 'error'


###### file name part function ######
def get_file_name_part(file_name):
    """
    Helper function for searching substring

    Args:
        file_name (string): The batch file name

    Returns:
        file_name_part (string): The batch file name part to key on
    """

    if 'transaction' in file_name:
        file_name_part = "TRAN."
    elif 'account' in file_name:
        file_name_part = "ACCT."
    elif 'cardholder' in file_name:
        file_name_part = "CARD."
    elif 'application' in file_name:
        #TODO determine file name from COF
        file_name_part = None
    else: 
        file_name_part = None

    return file_name_part


###### schema function ######
def get_schema(file_name):
    """
    Helper function to capture expected schema for batch files

    Args:
        file_name (string): The batch file to match schema

    Returns:
        schema (dictionary): The schema to validate against
    """
    to_datetime = lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    to_date = lambda d: datetime.strptime(d, '%Y-%m-%d')
    if 'transaction' in file_name:
        #TODO just including example of structure for now
        schema = {
            'integer_field': 
                {'required': True, 'type': 'integer'},
            'datetime_field':
                {'nullable': True, 'default': None, 'type': 'datetime', 'coerce': to_datetime},
            'date_field':
                {'nullable': True, 'default': None, 'type': 'datetime', 'coerce': to_date},
            'float_field':
                {'required': True, 'type': 'float'},
            'string_field':
                {'required': True, 'type': 'string'},
            'boolean_field':
                {'required': True, 'type': 'boolean'}
        }
    elif 'account' in file_name:
        schema = {}
    elif 'cardholder' in file_name:
        schema = {}
    elif 'application' in file_name:
        schema = {}
    else: 
        schema = {}

    return schema


###### lambda primary function ######
def lambda_handler(event, context):
    """
    This function validates batch file schema

    Args:
        event (object): Event data that's passed to the function upon execution
        context (object): Python objects that implements methods and has attributes
    
    Returns:
        NONE
    """
    try:
        # S3 object
        s3 = boto3.client('s3')

        # Event is result object from previous step
        key_name = event['key_name']
        bucket_name = event['bucket_name']
        bucket_arn = event['bucket_arn']
        
        logger_function(f"""Source Bucket Name: {bucket_name}""", type="info")
        logger_function(f"""Batch File Name: {key_name}""", type="info")

        #TODO Get schema
        #schema = get_schema(file_name)

        # Read the file from the source bucket
        response = s3.get_object(Bucket=bucket_name, Key=key_name)
        file_content = response['Body'].read()

        # Look in file name to key on what batch file it is, return aws glue job name
        job_name = contains_keywords(key_name)

        #TODO Create validator object
        #v = Validator(schema)
        #v.allow_unknown = False
        #v.require_all = True

        # Create result object to move to next step
        result = {}
        result['bucket_name'] = bucket_name
        result['bucket_arn'] = bucket_arn
        result['key_name'] = key_name
        result['job_name'] = job_name
        #TODO update with correct error bucket name
        result['error_bucket_name'] = "dev-fnt-0501651-batch-error-sandbox"
        result['archive_bucket_name'] = "dev-fnt-0501651-batch-stage-archive-sandbox"
        result['stage_bucket_name'] = "dev-fnt-0501651-batch-stage-sandbox"
        result['validation'] = "SUCCESS"
    except:
        result['validation'] = "FAILURE"
        result['reason'] = "Could not initiate validation"
        result['location'] = 'error'
        logger_function("ERROR: Could not initiate validation.", type="error")
        return(result)
    
    if job_name == 'error':
        result['validation'] = "FAILURE"
        result['reason'] = "Invalid batch file name"
        result['location'] = 'error'
        return(result)

    # Read file as df
    try:
        df_header = pd.read_csv(BytesIO(file_content), delimiter=',', nrows=1, header=None, engine='python')
        df_records = pd.read_csv(BytesIO(file_content), delimiter=',', skiprows=1, skipfooter=1, header=None, engine='python', on_bad_lines='skip')
        df_dict = df_records.to_dict(orient='records')
    except:
        result['validation'] = "FAILURE"
        result['reason'] = "Could not read batch file"
        result['location'] = 'error'
        logger_function("ERROR: Could not read batch file", type="error")
        return(result)
    
    # Validate there are records
    if len(df_dict) == 0:
        result['validation'] = "FAILURE"
        result['reason'] = "NO RECORD FOUND"
        result['location'] = 'error'
        logger_function("ERROR: File has no records", type="error")
        return(result)
    # Validate there is a file date
    try:
        batch_file_name = df_header[1][0]
        file_name_part = get_file_name_part(key_name)
        s_date_start = batch_file_name.find(file_name_part) + 5
        s_date_end = s_date_start + 8
        this_date = batch_file_name[s_date_start:s_date_end]
        s_time_start = batch_file_name.find(file_name_part) + 14
        s_time_end = s_time_start + 6
        this_time = batch_file_name[s_time_start:s_time_end]
        datetime_str = this_date+this_time
        datetime_object = str(datetime.strptime(datetime_str, '%Y%m%d%H%M%S'))
        result['latestBatchFileName'] = batch_file_name
        result['latestBatchTimestamp'] = datetime_object
    except:
        result['validation'] = "FAILURE"
        result['reason'] = "Missing valid file datetime"
        result['location'] = 'error'
        logger_function("ERROR: Missing valid file datetime", type="error")
        return(result)
    #TODO Validate column headers
    #TODO Validate records match schema

    if result['validation'] == "SUCCESS":
        logger_function(f"""Batch file {key_name} is valid""", type="info")
        logger_function("Moving batch file to next step.", type="info")
        return(result)
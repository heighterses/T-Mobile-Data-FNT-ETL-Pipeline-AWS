"""
This Lambda function validates batch file schema and records
"""

# packages
import json
import pandas as pd
from datetime import datetime
import boto3  # type: ignore
import os
from io import BytesIO
import logging
import io
import csv

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

    keywords = {
        'transaction': 'dev-fnt-0501651-glue-batch-transactions',
        'account': 'dev-fnt-0501651-glue-batch-account-summary',
        'cardholder': 'dev-fnt-0501651-glue-batch-cardholder',
        'application': 'dev-fnt-0501651-glue-batch-application'
    }

    if 'transaction' in s:
        return keywords['transaction']
    elif 'account' in s:
        return keywords['account']
    elif 'cardholder' in s:
        return keywords['cardholder']
    elif 'application' in s:
        return keywords['application']
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
        file_name_part = None
    else:
        file_name_part = None

    return file_name_part


###### schema function ######
def get_schema(file_name, bucket_name, schema_folder):
    """
    Helper function to fetch and return the schema from S3 based on the batch file name.

    Args:
        file_name (string): The batch file name to match schema (e.g., 'account', 'transaction')
        bucket_name (string): The S3 bucket where schemas are stored
        schema_folder (string): The folder path in S3 where schemas are located

    Returns:
        schema (list): The schema loaded from the corresponding CSV file
    """
    to_datetime = lambda d: datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    to_date = lambda d: datetime.strptime(d, '%Y-%m-%d')

    s3 = boto3.client('s3')

    # Define the schema file path based on the file type in the batch file name
    if 'transaction' in file_name:
        schema_key = f"{schema_folder}/transaction_schema.csv"
    elif 'account' in file_name:
        schema_key = f"{schema_folder}/account_schema.csv"
    elif 'cardholder' in file_name:
        schema_key = f"{schema_folder}/cardholder_schema.csv"
    elif 'application' in file_name:
        schema_key = f"{schema_folder}/application_schema.csv"

    schema = []

    # Open the schema CSV file and read its contents into a list
    try:
        # Fetch the schema file from S3
        response = s3.get_object(Bucket=bucket_name, Key=schema_key)
        schema_content = response['Body'].read().decode('utf-8')

        # Read the CSV content using csv.DictReader
        csv_reader = csv.DictReader(io.StringIO(schema_content))

        # Convert CSV rows into a dictionary, assuming schema has field names and types
        for row in csv_reader:
            field_name = row['field_name']
            schema_row = {}
            schema_row[field_name] = {
                'required': row['required'],
                'nullable': row['nullable'],
                'type': row['datatype'],
                'date_conversion': row['date_conversion'],
                'default': row['default']
            }

            # validate If the field is a date or datetime, 
            if row['datatype'] == 'datetime':
                schema_row[field_name]['validate'] = lambda value: to_datetime(value)
            elif row['datatype'] == 'date':
                schema_row[field_name]['validate'] = lambda value: to_date(value)
            else:
                # If it's not a date or datetime, no validation is added
                schema_row[field_name]['validate'] = lambda value: value  # No validation, pass the value as-is
            schema.append(schema_row)

        return schema

    except Exception as e:
        logger_function(f"Error reading schema file from S3: {e}", type="error")
        return []


def validate_column_headers(source_headers, schema_fields):
    """
    Validates if the column headers in the batch file match the schema.

    Args:
        source_headers (list): List of header columns from the batch file.
        schema_fields (list): List of expected field names from the schema.

    Returns:
        bool: True if headers match, False otherwise.
        errors (list): List of errors if headers do not match.
    """
    errors = []

    # Check if all schema field names are in the batch file headers
    for field in schema_fields:
        if field not in source_headers:
            errors.append(f"Missing field in batch file: {field}")

    # Check if there are extra fields in the batch file not in schema
    for field in source_headers:
        if field not in schema_fields:
            errors.append(f"Unexpected field in batch file: {field}")

    if errors:
        return False, errors
    else:
        return True, []


def validate_records(df_records, schema):
    """
    Validates the first 10 records in the batch file against the schema.

    Args:
        df_records (DataFrame): DataFrame containing batch file records.
        schema (list): List of dictionaries containing schema definitions.

    Returns:
        bool: True if records match the schema, False otherwise.
        errors (list): List of validation errors if records do not match.
    """
    errors = []

    # Convert schema list into a dictionary for easier lookups
    schema_dict = {}
    for field_schema in schema:
        for field_name, field_properties in field_schema.items():
            schema_dict[field_name] = field_properties

    # Check the first 10 records
    for index, record in df_records.iterrows():
        if index >= 10:
            break  # Only validate the first 10 records

        for field, value in record.items():
            if field in schema_dict:
                field_schema = schema_dict[field]

                # Check if the field is required and missing
                if field_schema['required'] == 'Yes' and pd.isnull(value):
                    errors.append(f"Record {index + 1}: {field} is required but missing.")

                # Check data type
                expected_type = field_schema['type']

                # Validate 'text' as string
                if expected_type == 'text' and not isinstance(value, str):
                    errors.append(f"Record {index + 1}: {field} should be a Text (String).")

                # Validate 'numeric' as integer or float
                elif expected_type == 'numeric' and not (isinstance(value, int) or isinstance(value, float)):
                    errors.append(f"Record {index + 1}: {field} should be a Numeric (Integer or Float).")

                # Validate 'date' as a valid date
                elif expected_type == 'date':
                    try:
                        pd.to_datetime(value, format='%Y-%m-%d', errors='raise')
                    except (ValueError, TypeError):
                        errors.append(f"Record {index + 1}: {field} should be a valid Date (YYYY-MM-DD).")

                # Validate 'timestamptz' as timestamp with timezone
                elif expected_type == 'timestamptz':
                    try:
                        pd.to_datetime(value, utc=True, errors='raise')
                    except (ValueError, TypeError):
                        errors.append(f"Record {index + 1}: {field} should be a valid Timestamp with Timezone (timestamptz).")

    if errors:
        return False, errors
    else:
        return True, []


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
        file_name = event['file_name']
        schema_bucket = event['schema_bucket']
        schema_folder = event['schema_folder']

        logger_function(f"""Source Bucket Name: {bucket_name}""", type="info")
        logger_function(f"""Batch File Name: {key_name}""", type="info")

        # Get schema
        schema = get_schema(file_name=file_name,
                            bucket_name=schema_bucket,
                            schema_folder=schema_folder)

        # Read the file from the source bucket
        response = s3.get_object(Bucket=bucket_name, Key=key_name)
        file_content = response['Body'].read()

        # Look in file name to key on what batch file it is, return aws glue job name
        job_name = contains_keywords(key_name)

        # Create result object to move to next step
        result = {}
        result['bucket_name'] = bucket_name
        result['bucket_arn'] = bucket_arn
        result['key_name'] = key_name
        result['job_name'] = job_name
        result['error_bucket_name'] = "dev-fnt-0501651-batch-error"
        result['archive_bucket_name'] = "dev-fnt-0501651-batch-archive"
        result['stage_bucket_name'] = "dev-fnt-0501651-batch-stage"
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
        df_source = pd.read_csv(BytesIO(file_content), delimiter=',')
        source_headers = df_source.columns
    except:
        result['validation'] = "FAILURE"
        result['reason'] = "Could not read batch file"
        result['location'] = 'error'
        logger_function("ERROR: Could not read batch file", type="error")
        return(result)

    # Validate there are records
    if df_source.count() == 0:
        result['validation'] = "FAILURE"
        result['reason'] = "NO RECORD FOUND"
        result['location'] = 'error'
        logger_function("ERROR: File has no records", type="error")
        return(result)

    # Validate column headers
    try:
        field_names = [list(e.keys())[0] for e in schema]
        response, errors = validate_column_headers(source_headers=source_headers, schema_fields=field_names)
        if not response:
            raise ValueError(f"Column header mismatch: {errors}")
    except Exception as e:
        result['validation'] = "FAILURE"
        result['reason'] = f"Header validation failed: {e}"
        result['location'] = 'error'
        logger_function(f"ERROR: Header validation failed: {e}", type="error")
        return result

    # Validate records match schema
    try:
        record_validation, record_errors = validate_records(df_source, schema)
        if not record_validation:
            raise ValueError(f"Record validation errors: {record_errors}")
    except Exception as e:
        result['validation'] = "FAILURE"
        result['reason'] = f"Record validation failed: {e}"
        result['location'] = 'error'
        logger_function(f"ERROR: Record validation failed: {e}", type="error")
        return result

    if result['validation'] == "SUCCESS":
        logger_function(f"Batch file {key_name} is valid", type="info")
        logger_function("Moving batch file to next step.", type="info")
        return result

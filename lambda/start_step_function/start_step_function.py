"""
This Lambda function initiates the AWS Step Functions for ETL orchestration
"""

# packages
import logging
import json
import boto3 # type: ignore
import os
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


###### lambda primary function ######
def lambda_handler(event, context):

    '''
    This function start AWS Step Functions

    Args:
        event (object): Event data that's passed to the function upon execution
        context (object): Python objects that implements methods and has attributes

    Returns:
        NONE
    '''
    try:
        # Read in SNS message
        sns_message = event['Records'][0]['Sns']['Message']

        # Get batch file name
        key_name = json.loads(sns_message)['Records'][0]['s3']['object']['key']
        bucket_name = json.loads(sns_message)['Records'][0]['s3']['bucket']['name']
        bucket_arn = json.loads(sns_message)['Records'][0]['s3']['bucket']['arn']
    
        # Create step function input object
        step_function_input = {}
        step_function_input['bucket_name'] = bucket_name
        step_function_input['bucket_arn'] = bucket_arn
        step_function_input['key_name'] = key_name
        logger_function("Sending JSON dump to Step Functions as input: ", type="info")
        logger_function(step_function_input, type="info")
        client = boto3.client('stepfunctions')
        # TODO parameterize hardcoded step_function_arn
        step_function_arn = 'arn:aws:states:us-west-2:905418049473:stateMachine:dev-fnt-0501651-state-machine-batch-etl'
        response = client.start_execution(stateMachineArn = step_function_arn, input= json.dumps(step_function_input))
    except:
        logger_function("ERROR: Could not initiate step functions.", type="error")
        sys.exit()
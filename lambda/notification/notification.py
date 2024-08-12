"""
This Lambda function creates the notification object
"""

# packages
import logging
import json
import boto3
import os

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
    """
    This function...

    Args:
        event (object): Event data that's passed to the function upon execution
        context (object): Python objects that implements methods and has attributes
    
    Returns:
        NONE
    """
    
    print(event)
    client = boto3.client('sns')
    #TODO update target arn
    response = client.publish(
        TargetArn='arn:aws:sns:us-west-2:730335645199:batch-etl-email',
        Message=json.dumps({'default': json.dumps(event)}),
        MessageStructure='json'
    )
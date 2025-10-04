"""
Document Intake Lambda Handler

This module handles the initial intake of documents uploaded to S3.
It creates metadata records in DynamoDB for tracking document processing status
and generates unique identifiers for each incoming document.

The handler is triggered by S3 upload events and serves as the entry point
for the document processing pipeline.

"""

import json
import boto3
import os
import uuid
from datetime import datetime
import urllib.parse

# Initialize AWS DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Environment variable for DynamoDB table name
TABLE_NAME = os.environ['METADATA_TABLE']

def lambda_handler(event, context):
    """
    Main Lambda handler function for document intake processing.
    
    Processes S3 upload events, creates metadata records, and initiates
    the document processing workflow.
    
    Args:
        event (dict): AWS Lambda event object containing S3 event records
        context (LambdaContext): AWS Lambda context object
        
    Returns:
        dict: Response object containing status code and document ID
    """
    # Extract S3 event information from the first record
    record = event['Records'][0]
    s3_info = record['s3']
    bucket = s3_info['bucket']['name']
    
    # Decode URL-encoded key (handles spaces and special characters in filenames)
    key = urllib.parse.unquote_plus(s3_info['object']['key'])
    size = s3_info['object'].get('size', 0)

    # Generate unique identifier and timestamp for the document
    document_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    original_file_name = os.path.basename(key)

    # Prepare metadata record for DynamoDB
    item = {
        'DocumentId': document_id,      # Unique identifier for the document
        'CreatedAt': now,              # UTC timestamp of upload
        'OriginalFileName': original_file_name,  # Original file name
        'S3Bucket': bucket,            # Source S3 bucket
        'S3Key': key,                  # S3 object key
        'FileSize': int(size),         # File size in bytes
        'Status': 'RECEIVED'           # Initial processing status
    }

    # Store metadata record in DynamoDB
    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)

    print(f"Successfully processed {key}. Record created with DocumentId: {document_id}")

    # Return success response with generated document ID
    return {'statusCode': 200, 'body': json.dumps({'DocumentId': document_id})}
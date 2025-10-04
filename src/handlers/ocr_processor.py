"""
OCR Processor Lambda Handler

This module processes documents stored in S3 using Amazon Textract for OCR (Optical Character Recognition).
It handles the extraction of text from various document formats (PDF, PNG, JPEG) and coordinates
with other components in the document processing pipeline.

Key responsibilities:
- Document text extraction using Amazon Textract
- Storage of extracted text in S3
- Status tracking in DynamoDB
- Notification dispatch for downstream processing

"""

import json
import boto3
import os

# Initialize AWS service clients
s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# Environment variables for AWS resources
METADATA_TABLE_NAME = os.environ['METADATA_TABLE']
PROCESSED_BUCKET_NAME = os.environ['PROCESSED_BUCKET']
INTERNAL_TOPIC_ARN = os.environ['INTERNAL_TOPIC_ARN']

def lambda_handler(event, context):
    """
    Main Lambda handler for OCR processing of documents.
    
    Processes DynamoDB stream events for new documents, performs OCR using Textract,
    stores extracted text, and triggers downstream classification.
    
    Args:
        event (dict): AWS Lambda event containing DynamoDB stream records
        context (LambdaContext): AWS Lambda context object
        
    Returns:
        dict: Response object with status code and message
    """
    table = dynamodb.Table(METADATA_TABLE_NAME)
    
    for record in event['Records']:
        # Skip non-INSERT events
        if record['eventName'] != 'INSERT':
            continue
        
        # Extract document metadata from DynamoDB stream
        new_image = record['dynamodb']['NewImage']
        document_id = new_image['DocumentId']['S']
        bucket = new_image['S3Bucket']['S']
        key = new_image['S3Key']['S']
        
        # Validate file type
        file_extension = os.path.splitext(key)[1].lower()
        if file_extension not in ['.png', '.jpg', '.jpeg', '.pdf']:
            # Update status for unsupported file types
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, FailureReason = :reason",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'FAILED', ':reason': 'Unsupported file type'}
            )
            continue

        try:
            # Perform OCR using Amazon Textract
            response = textract_client.detect_document_text(
                Document={'S3Object': {'Bucket': bucket, 'Name': key}}
            )
            detected_text = "".join(item["Text"] + "\n" for item in response["Blocks"] if item["BlockType"] == "LINE")
            
            # Store extracted text in S3
            text_s3_key = f"{document_id}.txt"
            s3_client.put_object(Bucket=PROCESSED_BUCKET_NAME, Key=text_s3_key, Body=detected_text.encode('utf-8'))
            
            # Update document status in DynamoDB
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, TextS3Key = :text_key",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'OCRED', ':text_key': text_s3_key}
            )

            # Trigger document classification via SNS
            message = {
                'DocumentId': document_id,
                'TextS3Key': text_s3_key,
                'OriginalFileName': new_image['OriginalFileName']['S']
            }
            sns_client.publish(
                TopicArn=INTERNAL_TOPIC_ARN,
                Message=json.dumps(message),
                Subject=f"OCR Complete for {document_id}"
            )
            print(f"Successfully processed and published message for {document_id}")

        except Exception as e:
            # Handle and log processing errors
            print(f"Error processing {key}: {str(e)}")
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, FailureReason = :reason",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'FAILED', ':reason': str(e)}
            )

    return {'statusCode': 200, 'body': 'OCR processing complete'}
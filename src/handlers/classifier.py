# src/handlers/classifier.py
"""
Document Classification Lambda Handler

This module processes extracted text documents and classifies them based on content analysis.
It's part of a serverless document processing pipeline that handles document classification
and notification dispatch.

"""

import json
import boto3
import os

# Initialize AWS service clients
s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

# Environment variables for AWS resources
METADATA_TABLE_NAME = os.environ['METADATA_TABLE']
PROCESSED_BUCKET_NAME = os.environ['PROCESSED_BUCKET']
USER_NOTIFICATION_TOPIC_ARN = os.environ['USER_NOTIFICATION_TOPIC_ARN']

def classify_document(text):
    """
    Classifies a document based on its text content.
    
    Args:
        text (str): The extracted text content from the document
        
    Returns:
        str: Classification result (INVOICE, RECEIPT, CONTRACT, or UNKNOWN)
    """
    text = text.lower()
    if "invoice" in text: return "INVOICE"
    if "receipt" in text: return "RECEIPT"
    if "agreement" in text or "contract" in text: return "CONTRACT"
    return "UNKNOWN"

def lambda_handler(event, context):
    """
    Main Lambda handler function for document classification.
    
    Processes SNS messages containing document information, classifies documents,
    updates DynamoDB records, and sends notifications to users.
    
    Args:
        event (dict): AWS Lambda event object containing SNS records
        context (LambdaContext): AWS Lambda context object
        
    Returns:
        dict: Response object with status code and message
    """
    table = dynamodb.Table(METADATA_TABLE_NAME)
    
    for record in event['Records']:
        try:
            # Parse SNS message and extract document metadata
            sns_message_body = record.get('Sns', {}).get('Message', '{}')
            message = json.loads(sns_message_body)
            
            # Safely extract required fields with fallback to None
            document_id = message.get('DocumentId')
            text_s3_key = message.get('TextS3Key')
            original_file_name = message.get('OriginalFileName')

            # Validate required fields
            if not all([document_id, text_s3_key, original_file_name]):
                print(f"CRITICAL ERROR: Discarding message with missing keys. Body: {sns_message_body}")
                continue

            print(f"Classifier processing DocumentId: {document_id}")

            # Retrieve and classify document text
            s3_object = s3_client.get_object(Bucket=PROCESSED_BUCKET_NAME, Key=text_s3_key)
            extracted_text = s3_object['Body'].read().decode('utf-8')

            doc_type = classify_document(extracted_text)
            print(f"Document {document_id} classified as: {doc_type}")

            # Update document metadata in DynamoDB
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, DocumentType = :doc_type",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'CLASSIFIED',':doc_type': doc_type}
            )
            
            # Prepare and send user notification
            final_message = (
                f"Document processing complete for: {original_file_name}\n\n"
                f"Document ID: {document_id}\n"
                f"Detected Type: {doc_type}"
            )
            sns_client.publish(
                TopicArn=USER_NOTIFICATION_TOPIC_ARN,
                Subject=f"Document Classified: {original_file_name}",
                Message=final_message
            )
            print(f"Final notification sent for {document_id}")

        except Exception as e:
            # Comprehensive error handling for any runtime exceptions
            print(f"CRITICAL ERROR: Unhandled exception while processing message. Discarding.")
            print(f"Error details: {str(e)}")
            if 'Sns' in record and 'Message' in record['Sns']:
                print(f"Message Body: {record['Sns']['Message']}")
            continue
            
    return {'statusCode': 200, 'body': 'Classification run complete'}
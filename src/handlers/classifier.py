# src/handlers/classifier.py (Final, Resilient Version)
import json
import boto3
import os

s3_client = boto3.client('s3')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

METADATA_TABLE_NAME = os.environ['METADATA_TABLE']
PROCESSED_BUCKET_NAME = os.environ['PROCESSED_BUCKET']
USER_NOTIFICATION_TOPIC_ARN = os.environ['USER_NOTIFICATION_TOPIC_ARN']

def classify_document(text):
    text = text.lower()
    if "invoice" in text: return "INVOICE"
    if "receipt" in text: return "RECEIPT"
    if "agreement" in text or "contract" in text: return "CONTRACT"
    return "UNKNOWN"

def lambda_handler(event, context):
    table = dynamodb.Table(METADATA_TABLE_NAME)
    
    for record in event['Records']:
        try:
            sns_message_body = record.get('Sns', {}).get('Message', '{}')
            message = json.loads(sns_message_body)
            
            # Use .get() for safe access to dictionary keys
            document_id = message.get('DocumentId')
            text_s3_key = message.get('TextS3Key')
            original_file_name = message.get('OriginalFileName')

            # If any essential key is missing, discard the message
            if not all([document_id, text_s3_key, original_file_name]):
                print(f"CRITICAL ERROR: Discarding message with missing keys. Body: {sns_message_body}")
                continue

            print(f"Classifier processing DocumentId: {document_id}")

            s3_object = s3_client.get_object(Bucket=PROCESSED_BUCKET_NAME, Key=text_s3_key)
            extracted_text = s3_object['Body'].read().decode('utf-8')

            doc_type = classify_document(extracted_text)
            print(f"Document {document_id} classified as: {doc_type}")

            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, DocumentType = :doc_type",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'CLASSIFIED',':doc_type': doc_type}
            )
            
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
            # This block will now catch any unexpected error (like timeouts, S3 errors, etc.)
            print(f"CRITICAL ERROR: Unhandled exception while processing message. Discarding.")
            print(f"Error details: {str(e)}")
            # Log the raw message body for debugging
            if 'Sns' in record and 'Message' in record['Sns']:
                print(f"Message Body: {record['Sns']['Message']}")
            continue
            
    return {'statusCode': 200, 'body': 'Classification run complete'}
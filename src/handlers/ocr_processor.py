# src/handlers/ocr_processor.py (Decoupled Version)
import json
import boto3
import os

s3_client = boto3.client('s3')
textract_client = boto3.client('textract')
sns_client = boto3.client('sns')
dynamodb = boto3.resource('dynamodb')

METADATA_TABLE_NAME = os.environ['METADATA_TABLE']
PROCESSED_BUCKET_NAME = os.environ['PROCESSED_BUCKET']
INTERNAL_TOPIC_ARN = os.environ['INTERNAL_TOPIC_ARN']

def lambda_handler(event, context):
    table = dynamodb.Table(METADATA_TABLE_NAME)
    for record in event['Records']:
        if record['eventName'] != 'INSERT':
            continue
        
        new_image = record['dynamodb']['NewImage']
        document_id = new_image['DocumentId']['S']
        bucket = new_image['S3Bucket']['S']
        key = new_image['S3Key']['S']
        
        file_extension = os.path.splitext(key)[1].lower()
        if file_extension not in ['.png', '.jpg', '.jpeg', '.pdf']:
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, FailureReason = :reason",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'FAILED', ':reason': 'Unsupported file type'}
            )
            continue

        try:
            response = textract_client.detect_document_text(
                Document={'S3Object': {'Bucket': bucket, 'Name': key}}
            )
            detected_text = "".join(item["Text"] + "\n" for item in response["Blocks"] if item["BlockType"] == "LINE")
            
            text_s3_key = f"{document_id}.txt"
            s3_client.put_object(Bucket=PROCESSED_BUCKET_NAME, Key=text_s3_key, Body=detected_text.encode('utf-8'))
            
            # Update DynamoDB with text key and OCRED status
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, TextS3Key = :text_key",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'OCRED', ':text_key': text_s3_key}
            )

            # --- KEY CHANGE: Publish a message to SNS for the classifier ---
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
            print(f"Error processing {key}: {str(e)}")
            table.update_item(
                Key={'DocumentId': document_id},
                UpdateExpression="SET #s = :status, FailureReason = :reason",
                ExpressionAttributeNames={'#s': 'Status'},
                ExpressionAttributeValues={':status': 'FAILED', ':reason': str(e)}
            )

    return {'statusCode': 200, 'body': 'OCR processing complete'}
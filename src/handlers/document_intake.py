# src/handlers/document_intake.py
import json
import boto3
import os
import uuid
from datetime import datetime
import urllib.parse

dynamodb = boto3.resource('dynamodb')
TABLE_NAME = os.environ['METADATA_TABLE']

def lambda_handler(event, context):
    # Get the S3 bucket and key from the event
    record = event['Records'][0]
    s3_info = record['s3']
    bucket = s3_info['bucket']['name']
    # Key can have spaces or special characters, so we unquote it
    key = urllib.parse.unquote_plus(s3_info['object']['key'])
    size = s3_info['object'].get('size', 0)

    document_id = str(uuid.uuid4())
    now = datetime.utcnow().isoformat()
    original_file_name = os.path.basename(key)

    item = {
        'DocumentId': document_id,
        'CreatedAt': now,
        'OriginalFileName': original_file_name,
        'S3Bucket': bucket,
        'S3Key': key,
        'FileSize': int(size),
        'Status': 'RECEIVED'
    }

    table = dynamodb.Table(TABLE_NAME)
    table.put_item(Item=item)

    print(f"Successfully processed {key}. Record created with DocumentId: {document_id}")

    return {'statusCode': 200, 'body': json.dumps({'DocumentId': document_id})}
import json
import os
import boto3

s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

QUEUE_URL = os.environ['SQS_QUEUE_URL']

def lambda_handler(event, context):
    for record in event.get('Records', []):
        bucket_name = record['s3']['bucket']['name']
        object_key = record['s3']['object']['key']
        
        # Lê o conteúdo enviado ao S3 de origem
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        content = response['Body'].read().decode('utf-8')
        
        try:
            data = json.loads(content)
            items = data if isinstance(data, list) else [data]
        except json.JSONDecodeError:
            items = [{'linha': line} for line in content.splitlines() if line.strip()]
        
        # Envia cada item individualmente para a Fila SQS
        for item in items:
            sqs_client.send_message(
                QueueUrl=QUEUE_URL,
                MessageBody=json.dumps(item)
            )
            
    return {'statusCode': 200, 'body': 'Mensagens enviadas para SQS'}
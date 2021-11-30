import json
import boto3
from decimal import Decimal


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return str(obj)
        return json.JSONEncoder.default(self, obj)


def lambda_handler(event, context):

    method = event['context']['http-method']

    print(event)
    if method == 'GET':
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table('Customers')
        key = event['params']['querystring']['CustomerID']
        response = table.get_item(Key={'CustomerID': int(key)})

        return {
            'statusCode': 200,
            'body': json.dumps(response, cls=DecimalEncoder)
        }

    elif method == 'POST':
        kinesis = boto3.client('kinesis')
        record_str = json.dumps(event['body-json'])

        response = kinesis.put_record(
            StreamName='APIData',
            Data=record_str,
            PartitionKey='string'
        )

        return {
            'statusCode': 200,
            'body': record_str
        }

    else:
        return {
            'statusCode': 501,
            'body': 'Not implemented'
        }

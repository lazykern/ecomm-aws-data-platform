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

    if method == 'POST':
        kinesis = boto3.client('kinesis')
        
        data = event['body-json']
        
        if isinstance(data, list):
            for row in data:
                record_str = json.dumps(row)

                response = kinesis.put_record(
                    StreamName='APIData',
                    Data=record_str,
                    PartitionKey='string'
                )
                
        elif isinstance(data, dict):
            record_str = json.dumps(row)

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

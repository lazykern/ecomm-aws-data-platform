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

    if method == 'GET':

        table = boto3.resource('dynamodb').Table('Invoices')

        params = event

        key = event['params']['querystring']['InvoiceNo']

        response = table.get_item(Key={'InvoiceNo': str(key)})

        return {
            'statusCode': 200,
            'body': json.dumps(response, cls=DecimalEncoder)
        }
    else:
        return {
            'statusCode': 501,
            'body': 'Not implemented'
        }

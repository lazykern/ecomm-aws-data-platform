import base64
import boto3
from datetime import datetime

s3_client = boto3.client('s3')

dateTimeObj = datetime.now()

timestampStr = dateTimeObj.strftime("%d-%b-%Y-%H%M%S")

kinesisRecords = []


def lambda_handler(event, context):
    
    for record in event['Records']:
        payload = base64.b64decode(record['kinesis']['data'])
        kinesisRecords.append(str(payload))

    ex_string = '\n'.join(kinesisRecords)

    mykey = 'output-' + timestampStr + '.txt'

    response = s3_client.put_object(Body=ex_string, Bucket='ecommerce-customer-datalake', Key=mykey)

    return 'Successfully processed {} records.'.format(len(event['Records']))

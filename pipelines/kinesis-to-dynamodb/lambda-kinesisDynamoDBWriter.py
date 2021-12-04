import json
import base64
import boto3
from decimal import Decimal
from datetime import datetime


def lambda_handler(event, context):

    dynamodb = boto3.resource('dynamodb')
    invoices_table = dynamodb.Table('Invoices')
    customers_table = dynamodb.Table('Customers')

    for record in event['Records']:

        t_record = base64.b64decode(record['kinesis']['data'])

        str_record = str(t_record, 'utf-8')

        data = json.loads(str_record, parse_float=Decimal)

        invoice_key = data["InvoiceNo"]
        invoice_date = data["InvoiceDate"]
        customer_key = data["CustomerID"]
        country = data["Country"]

        try:
            invoices_table.put_item(
                Item={
                    "InvoiceNo": invoice_key,
                    "InvoiceDate": invoice_date,
                    "Stocks": {}
                },
                ConditionExpression='attribute_not_exists(InvoiceNo)'
            )
        except:
            print(f"Invoice #{invoice_key} already exists")

        try:
            customers_table.put_item(
                Item={
                    "CustomerID": customer_key,
                    "Country": country,
                    "InvoiceNoArr": []
                },
                ConditionExpression='attribute_not_exists(CustomerID)'
            )
        except:
            print(f"Customer #{invoice_key} already exists")

        customers_table.update_item(
            Key={
                "CustomerID": customer_key
            },
            UpdateExpression="SET InvoiceNoArr = list_append(InvoiceNoArr, :i)",
            ExpressionAttributeValues={
                ':i': [invoice_key],
            },
            ReturnValues="UPDATED_NEW"
        )


        stockcode = data.pop("StockCode", None)

        data.pop("InvoiceNo", None)
        data.pop("CustomerID", None)
        data.pop("InvoiceDate", None)
        data.pop("Country", None)

        invoices_table.update_item(
            Key={
                "InvoiceNo": invoice_key
            },
            UpdateExpression="SET Stocks.#stock = :stock",
            ExpressionAttributeNames={
                "#stock": stockcode
            },
            ExpressionAttributeValues={
                ":stock": {
                    **data
                }
            },
            ReturnValues="UPDATED_NEW"
        )

    return f"Successfully processed {len(event['Records'])} records."

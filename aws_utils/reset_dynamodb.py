import boto3
from my_aws_package import set_aws_session, dynamodb_table_names



def reset_dynamodb():
    dynamodb = boto3.resource("dynamodb")
    for table_name in dynamodb_table_names:
        table = dynamodb.Table(table_name)
        scan = table.scan()
        key = table.key_schema[0]["AttributeName"]
        
        if not scan['Items']: return
        
        print(f"Deleting all item in {table_name}")
        with table.batch_writer() as batch:
            for each in scan['Items']:
                batch.delete_item(
                    Key={
                        key: each[key]
                    }
                )
        print(f"Deleted all item in {table_name} ({len(scan['Items'])})")

if __name__ == '__main__':
    set_aws_session()
    reset_dynamodb()
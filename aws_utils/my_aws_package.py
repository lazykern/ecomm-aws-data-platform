import boto3
from dotenv import dotenv_values

AWS_CONFIG = raw = __dict__ = dotenv_values("aws_utils/aws.env")
params = {k: __dict__[k] for k in list(__dict__)[:3]}
s3_bucket_name = __dict__["s3_bucket_name"]
dynamodb_table_names = __dict__["dynamodb_table_names"].split(",")

def set_aws_session():
    boto3.setup_default_session(**params)

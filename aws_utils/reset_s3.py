import boto3
from my_aws_package import set_aws_session, s3_bucket_name

set_aws_session()

def reset_s3():
    boto3.resource("s3").Bucket(s3_bucket_name).objects.all().delete()
    print("All objects deleted from bucket: {}".format(s3_bucket_name))

if __name__ == '__main__':
    reset_s3()
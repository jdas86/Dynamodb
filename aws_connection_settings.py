import boto3

aws_access_key_id = 'AKIARLHARGGRP572Q3KO'
aws_secret_access_key = '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'
region_name = 'us-east-2'

s3_client = boto3.client('s3',
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key,
                 region_name=region_name)


s3_resource = boto3.resource('s3',
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key,
                 region_name=region_name)

ddb_resource = boto3.resource('dynamodb',
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key,
                 region_name=region_name)

ddb_client = boto3.client('dynamodb',
                 aws_access_key_id=aws_access_key_id,
                 aws_secret_access_key=aws_secret_access_key,
                 region_name=region_name)

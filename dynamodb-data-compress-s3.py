import boto3
import json
import zlib
import gzip
import snappy
import pandas as pd
from aws_connection_settings import ddb_resource, s3_client, ddb_client

class S3ToDynamoDB:
    def __init__(self, bucket_name, file_key, table_name):
        self.bucket_name = bucket_name
        self.file_key = file_key
        self.table_name = table_name
        self.s3_client = s3_client
        self.dynamodb = ddb_resource

    def read_file_from_s3(self):
        try:
            obj = self.s3_client.get_object(Bucket=self.bucket_name, Key=self.file_key)
            json_data = json.loads(obj['Body'].read().decode('utf-8'))
            return json_data
        except Exception as e:
            print(f"Error reading file from S3: {e}")
            return None

    def compress_data(self, df_data):
        df_data = df_data.applymap(lambda x: gzip.compress(str(x).encode()))
        return df_data.to_dict(orient="records")

    #Writing data to dynamodb table
    def write_to_dynamodb(self):
        for item in compressed_data:
            ddb_client.put_item(
                TableName=self.table_name,
                Item={
                    "id": {"B": item["id"]},
                    "type": {"B": item["type"]},
                    "actor": {"B": item["actor"]},
                    "repo": {"B": item["repo"]},
                    "payload": {"B": item["payload"]},
                    "public": {"B": item["public"]},
                    "created_at": {"B": item["created_at"]},
                    "org": {"B": item["org"]},
                }
            )

if __name__ == "__main__":
    bucket_name = 'jd-aws-proj-bucket'
    file_key = 'recovery-src-data/large-file.json'
    table_name = 'event_tbl'

    s3_to_dynamodb = S3ToDynamoDB(bucket_name, file_key, table_name)
    json_data = s3_to_dynamodb.read_file_from_s3()

    if json_data:
        df_data = pd.DataFrame(json_data)
        compressed_data = s3_to_dynamodb.compress_data(df_data)
        s3_to_dynamodb.write_to_dynamodb()
    #     print(f"Data from S3 successfully compressed and stored in DynamoDB table '{table_name}'.")

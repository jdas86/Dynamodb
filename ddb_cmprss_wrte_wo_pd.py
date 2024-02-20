import json
import gzip
from aws_connection_settings import s3_client, ddb_resource
import sys

columns_to_compress = ['actor', 'repo', 'payload']
# columns_to_compress = sys.argv[4]
AFFILIATION_ISTRUE = True
# AFFILIATION_ISTRUE = sys.argv[5]


class Dynamodb_compression:

    bucket_name = 'jd-aws-proj-bucket'
    # bucket_name = sys.argv[1]
    file_key = 'recovery-src-data/large-file.json'
    # file_key = sys.argv[2]
    table_name = 'event_tbl'
    # table_name = sys.argv[3]
    s3_client = s3_client
    dynamodb = ddb_resource
    table = dynamodb.Table(table_name)
    obj = s3_client.get_object(Bucket=bucket_name, Key=file_key)
    json_data = json.loads(obj['Body'].read().decode('utf-8'))
    
    def compress_columns_affl(data, columns_to_compress):
        compressed_data = []
        for item in data:
            item_copy = item.copy()
            for column in columns_to_compress:
                column_data = item_copy.pop(column)
                column_json = json.dumps(column_data)
                item_copy[column] = gzip.compress(column_json.encode("utf-8"))
            compressed_data.append(item_copy)
        return compressed_data


    if __name__ == "__main__":
    # # Print compressed data
    # for compressed_item in compressed_sample_data:
    #     print(compressed_item)
        if AFFILIATION_ISTRUE:
            compressed_sample_data = compress_columns_affl(json_data, columns_to_compress)
            with table.batch_writer() as batch:
                for item in compressed_sample_data:
                    batch.put_item(Item=item)        
        else:
            print("Provide your affiliation type")



# py ddb_cmprss_wrte_wo_pd.py 'jd-aws-proj-bucket' 'recovery-src-data/large-file.json' 'event_tbl' ['actor', 'repo', 'payload'] True
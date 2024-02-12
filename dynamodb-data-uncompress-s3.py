# import boto3
# import json
# import zlib
# import gzip
# import snappy
# import pandas as pd
# from aws_connection_settings import ddb_resource, s3_client, ddb_client

# class DynamoDBRead:
#     def __init__(self, table_name):
#         self.table_name = table_name
#         self.s3_client = s3_client
#         self.dynamodb = ddb_resource

#     def reading_dynamoddb(self):
#         #Reading data from dynamodb table
#         response = ddb_client.scan(TableName=self.table_name)
#         items = response["Items"]
#         # Check if there are any items
#         if items:
#             # Create a list of items, ensuring "actor" values are bytes
#             items_list = [{"id": item["id"]["B"], "actor": item["actor"]["B"]} for item in items]
#             # Create pandas DataFrame, decompressing the "actor_new" column
#             df = pd.DataFrame(items_list).assign(
#             actor_new=lambda df: df["actor"].apply(lambda x: gzip.decompress(x).decode()))

#             # Drop the compressed column if no longer needed
#             df = df.drop("actor", axis=1)
#             df = df.rename(columns={'actor_new': 'actor'})
#             # Access data in the DataFrame
#             print(df.head())
#             # print(df.info())
#         else:
#             print("No data found in the table.")

#     #Writing data to dynamodb table

# if __name__ == "__main__":
#     table_name = 'event_tbl'

#     dynamodbread = DynamoDBRead(table_name)
#     json_data = dynamodbread.reading_dynamoddb()





import boto3
import json
import zlib
import gzip
import snappy
import pandas as pd
from aws_connection_settings import ddb_resource, s3_client, ddb_client

class DynamoDBRead:
    def __init__(self, table_name):
        self.table_name = table_name
        self.s3_client = s3_client
        self.dynamodb = ddb_client

    def get_all_attribute_names(self):
        response = self.dynamodb.describe_table(TableName=self.table_name)
        attribute_names = response["Table"]["AttributeDefinitions"]
        return [attr["AttributeName"] for attr in attribute_names]

    def reading_dynamoddb(self):
        response = ddb_client.scan(TableName=self.table_name)
        items = response["Items"]

        if items:
            attribute_names = self.get_all_attribute_names()
            items_list = []
            for item in items:
                item_dict = {}
                for attr_name in attribute_names:
                    attr_value = item.get(attr_name, {}).get("B")
                    if attr_value:
                        item_dict[attr_name] = gzip.decompress(attr_value).decode()
                items_list.append(item_dict)
            df = pd.DataFrame(items_list)
        else:
            print("No data found in the table.")

if __name__ == "__main__":
    table_name = 'event_tbl'
    dynamodbread = DynamoDBRead(table_name)
    dynamodbread.reading_dynamoddb()

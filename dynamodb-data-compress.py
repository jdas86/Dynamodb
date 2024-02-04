import boto3
import gzip
import datetime
import json
import io
from aws_connection_settings import ddb_client, s3_client, s3_resource, ddb_resource
import pandas
import snappy
import zlib
import os

compression_col_list = ['actor', 'repo', 'payload', 'org']

class Dynamodb_compression:
    
    def __init__(self) -> None:
        #Reading data and converting to dataframe
        with open("Dynamodb-project/large-file.json", "r") as f:
            self.data = json.load(f)
        
        # Calculate the size of the JSON data in bytes
        data_bytes = json.dumps(self.data).encode()
        data_size_kb = round(len(data_bytes) / 1024, 2)
        print(f"Json file size in Kbs: {data_size_kb}")

        #Convert to dataframe
        self.df_data = pandas.DataFrame(self.data)
        self.table_name = "event_tbl"


    # Doing compression for the nested dict columns
    def compression_process(self, compression_col_list):
        for col_list in compression_col_list:
            self.df_data[col_list] = self.df_data[col_list].apply(lambda x: zlib.compress(str(x).encode()))
        self.items_dict = self.df_data.to_dict(orient="records")

    # return items_dict


    #Writing data to dynamodb table
    def write_to_dynamodb(self):
        self.compression_process(compression_col_list)
        for item in self.items_dict:
            ddb_client.put_item(
                TableName=self.table_name,
                Item={
                    "id": {"S": str(item["id"])},
                    "id": {"S": str(item["id"])},
                    "actor": {"B": item["actor"]},
                    "repo": {"B": item["actor"]},
                    "payload": {"B": item["actor"]},
                    "public": {"S": str(item["id"])},
                    "create_at": {"S": str(item["id"])},
                    "org": {"B": item["actor"]},
                }
            )



    def reading_dynamoddb(self):
        #Reading data from dynamodb table
        response = ddb_client.scan(TableName=self.table_name)
        items = response["Items"]
        # Check if there are any items
        if items:
            # Create a list of items, ensuring "actor" values are bytes
            items_list = [{"id": item["id"]["S"], "actor": item["actor"]["B"]} for item in items]
            # Create pandas DataFrame, decompressing the "actor_new" column
            df = pandas.DataFrame(items_list).assign(
            actor_new=lambda df: df["actor"].apply(lambda x: zlib.decompress(x).decode()))

            # Drop the compressed column if no longer needed
            df = df.drop("actor", axis=1)
            df = df.rename(columns={'actor_new': 'actor'})
            # Access data in the DataFrame
            print(df.head())
            # print(df.info())
        else:
            print("No data found in the table.")


dynamodb_compression = Dynamodb_compression()
# dynamodb_compression.compression_process(compression_col_list)
# dynamodb_compression.write_to_dynamodb()
dynamodb_compression.reading_dynamoddb()

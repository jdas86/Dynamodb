from aws_connection_settings import ddb_client, s3_client, s3_resource, ddb_resource
import json
import pandas
import io

# Specify the bucket name and file key
bucket_name = "jd-aws-proj-bucket"
file_key = "Failed-S3/event-sample-data.json"

# Read the file from S3
response = s3_client.get_object(Bucket=bucket_name, Key=file_key)
read_data = response['Body'].read().decode('utf-8')

#read as json
read_df = pandas.read_json(read_data)

#drop the members column
new_df = read_df.drop('members', axis=1)
new_df['members']= pandas.NA

new_event_df = new_df.drop_duplicates(subset=["squadId"]) 

#Flag column created
s3enabledflag = {
        "isenabled": True, 
        "s3location": "jd-aws-proj-bucket/Failed-S3/event-sample-data.json"
        }


new_event_df['s3enabledflag'] = new_event_df.apply(lambda row: s3enabledflag, axis=1)

print(new_event_df)

#Write to json, to s3
json_buffer = io.StringIO()
new_event_df.to_json(json_buffer, orient='records')
object_key = 'Failed-S3/event-json-data.json'
s3_resource.Object(bucket_name, object_key).put(Body=json_buffer.getvalue())

json_object = s3_client.get_object(Bucket= bucket_name, Key= object_key)
json_dict = json.load(json_object['Body'])

#Write to dynamodb table
table = ddb_resource.Table('eventdata_tbl')
item_to_put = json_dict[0]  
response1 = table.put_item(Item=item_to_put)


#Jasobanta Das



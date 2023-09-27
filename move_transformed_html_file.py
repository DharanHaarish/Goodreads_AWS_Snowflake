import json
import boto3
import datetime

s3 = boto3.client('s3')

def lambda_handler(event, context):
    
    bucket = "goodreads-top-2023-raw"
    folder_name = "before"
    
    destination_folder = "after"
    destination_file_name = "raw_data_" + str(datetime.date.today()) +".html"
    
    obj = s3.list_objects_v2(Bucket = bucket, Prefix = f'{folder_name}/')
    for i in obj.get('Contents'):
        if i.get('Key').endswith('.html'):
            obj_key = i.get('Key')
            
    copy_source = {'Bucket': bucket, 'Key': obj_key}
    s3.copy_object(CopySource=copy_source, Bucket=bucket, Key=f'{destination_folder}/{destination_file_name}')
    
    s3.delete_object(Bucket=bucket, Key=obj_key)

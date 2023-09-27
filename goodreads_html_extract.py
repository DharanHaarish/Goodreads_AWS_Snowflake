import boto3
import requests
from datetime import date 

s3 = boto3.client('s3')

def lambda_handler(event, context):
  
    url = 'https://www.goodreads.com/list/show/183940.Best_Books_of_2023'

    response = requests.get(url)

    if response.status_code == 200:
        html_content = response.text


        bucket_name = 'goodreads-top-2023-raw'
        folder_name = 'before'
        file_name = 'raw_data_'+str(date.today())+'.html'
        
        key = f'{folder_name}/{file_name}'

        s3.put_object(Bucket=bucket_name, Key=key, Body=html_content)

        return {
            'statusCode': 200,
            'body': 'HTML content successfully fetched and uploaded to S3.'
        }
        
    else:
        return {
            'statusCode': response.status_code,
            'body': 'Failed to fetch HTML content from the URL.'
        }

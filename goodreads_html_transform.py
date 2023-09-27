import boto3
from bs4 import BeautifulSoup
from googleapiclient.discovery import build
import csv
import os
import json

s3 = boto3.client('s3')

def html_transform(html_content):
    
    soup = BeautifulSoup(html_content, 'html.parser')
    tr_items = soup.find_all('tr', itemscope=True)
    titles = [a['title'] for tr in tr_items for a in tr.find_all('a', title=True)]
    
    book_name = titles[::6]
    gb_api = os.environ.get('google_books_api')
    service = build('books', 'v1', developerKey=gb_api)
    
    author_name = list()
    publisher_name = list()
    published_date = list()
    book_description = list()
    book_genre = list()
    
    for i in book_name:
        query = f'intitle:"{i}"'
        results = service.volumes().list(q=query).execute()
        items = results.get('items', [])
        
        if items:
            authors = items[0]['volumeInfo'].get('authors', [])
            publisher = items[0]['volumeInfo'].get('publisher',[])
            date = items[0]['volumeInfo'].get('publishedDate', [])
            description = items[0]['volumeInfo'].get('description', [])
            genre = items[0]['volumeInfo'].get('categories', [])
            author_name.append(authors)
            publisher_name.append(publisher)
            published_date.append(date)
            book_description.append(description)
            book_genre.append(genre)
            
        else:
            author_name.append([])
            publisher_name.append([])
            published_date.append([])
            book_description.append([])
            book_genre.append([])
    
    book_genre = [genre[0] if genre else None for genre in book_genre]
    author_name = [','.join(sub_authors) if sub_authors else None for sub_authors in author_name]
    
    bk_json = json.dumps(book_name)
    auth_json = json.dumps(author_name)
    pblr_json = json.dumps(publisher_name)
    pbldt_json = json.dumps(published_date)
    desc_json = json.dumps(book_description)
    gnr_json = json.dumps(book_genre)
        
    to_bucket = "goodreads-top-2023-transformed"
    book_key = "books.json"
    author_key = "authors.json"
    publisher_key = "publishers.json"
    date_key = "dates.json"
    desc_key = "description.json"
    genre_key = "genre.json"
    
    s3.put_object(Bucket = to_bucket, Key = book_key, Body = bk_json)
    s3.put_object(Bucket = to_bucket, Key = author_key, Body = auth_json)
    s3.put_object(Bucket = to_bucket, Key = publisher_key, Body = pblr_json)
    s3.put_object(Bucket = to_bucket, Key = date_key, Body = pbldt_json)
    s3.put_object(Bucket = to_bucket, Key = desc_key, Body = desc_json)
    s3.put_object(Bucket = to_bucket, Key = genre_key, Body = gnr_json)
    
def lambda_handler(event, context):
    
    bucket_name = "goodreads-top-2023-raw"
    folder_name = "before"
    
    obj = s3.list_objects_v2(Bucket=bucket_name, Prefix=f'{folder_name}/')
    for i in obj.get('Contents'):
        if i.get('Key').endswith('.html'):
            obj_key = i.get('Key')
            
    
    file = s3.get_object(Bucket = bucket_name, Key = obj_key)
    html_content = file['Body'].read().decode('utf-8')
        
    html_transform(html_content)

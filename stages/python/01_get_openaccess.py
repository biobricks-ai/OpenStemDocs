import sys
import glob
from pathlib import Path
import hashlib
import multiprocessing
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from datetime import date, datetime, timedelta
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import fastparquet
import json

# Get last processed date  
def get_last_processed_date():
    last_processed_file = Path('last_processed_date.txt')
    if last_processed_file.exists():
        with open(last_processed_file, 'r') as file:
            return datetime.strptime(file.read(), "%Y-%m-%d").date()
    else:
        print("No last processed date found, using 224 years before today starting from Jan 1 (testing for now)")
        today = date.today()
        start_date = date(today.year - 224, 1, 1)
        return start_date


# Save last processed date into a file
def save_last_processed_date(processed_date):
    with open('last_processed_date.txt', 'w') as file:
        file.write(processed_date.strftime("%Y-%m-%d"))

# Use previous two months from today as the last processed date
last_processed_date = get_last_processed_date()
print(last_processed_date)


# Directory of files to process
raw_path = Path('output')
raw_path.mkdir(exist_ok=True)

# List S3 files
def s3_run(bucket, prefix):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key'], obj['LastModified'].date()

# Filter files by last processed date
filtered_files = [(key, date) for key, date in s3_run('openalex', 'data/works/') if date >= last_processed_date]


# Process each file 
def process_file(file_info):
    key, _ = file_info
    filename = f"s3://openalex/{key}"

    hash_value = hashlib.md5(key.encode()).hexdigest()
    group = int(hash_value, 16) % 4 

    outfile = f"group_{group:02d}.parquet"
    outpath = raw_path / outfile
    
    df = pd.read_json(filename, lines=True, chunksize=10000000)

    for chunk in df:
        chunk['publication_date'] = pd.to_datetime(chunk['publication_date'])
        chunk['url'] = chunk['best_oa_location'].apply(lambda x: x.get('pdf_url') if isinstance(x, dict) else None)
        chunk['url'] = chunk['url'].str.split(',').str[0].str.strip()

        filtered_chunk = chunk[(chunk['url'].notna()) & (chunk['url'] != 'null') & 
            (chunk['publication_date'] >= pd.to_datetime(last_processed_date)) &
            (chunk['doi'].str.contains('doi.org', case=False, na=False))][['doi', 'url', 'publication_date']]

        
        filtered_chunk = filtered_chunk.drop_duplicates(subset='doi', keep='first')
        #filtered_chunk = filtered_chunk.drop_duplicates(subset='title', keep='first')


        if outpath.exists():
            filtered_chunk.to_parquet(outpath, engine='fastparquet', compression='snappy', append=True)
        else:
            filtered_chunk.to_parquet(outpath, engine='fastparquet', compression='snappy')

    return 1


# Use multiprocessing to process files
with ProcessPoolExecutor(max_workers=12) as executor:
    executor.map(process_file, filtered_files)


# Calculate the new processed date
all_parquet_files = list(raw_path.glob('*.parquet'))
if all_parquet_files:
    df_list = [pd.read_parquet(file) for file in all_parquet_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    new_processed_date = combined_df['publication_date'].max().date()
    save_last_processed_date(new_processed_date)
    print(f"Processed publications from {last_processed_date} to: {new_processed_date}")
else:
    print("No new data processed.")
    new_processed_date = last_processed_date

save_last_processed_date(new_processed_date)
   
   






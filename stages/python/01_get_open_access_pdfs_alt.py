import sys
import glob
from pathlib import Path
import subprocess
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import boto3
from botocore import UNSIGNED
from botocore.client import Config
import pandas as pd
import fastparquet
import json

# Get last processed date  
# If there is no last processed date, use the date of one month before today. 
def get_last_processed_date():
    last_processed_file = Path('last_processed_date.txt')
    if last_processed_file.exists():
        with open(last_processed_file, 'r') as file:
            return datetime.strptime(file.read(), "%Y-%m-%d").date()
    else:
        print("No last processed date found, using 2 weeks before today.")
        return date.today() - timedelta(days=14)

# Save last processed date into a file
def save_last_processed_date(processed_date):
    with open('last_processed_date.txt', 'w') as file:
        file.write(processed_date.strftime("%Y-%m-%d"))

# Use previous two months from today as the last processed date
last_processed_date = get_last_processed_date()
print(last_processed_date)
# Directory of files to process
raw_path = Path('download')
raw_path.mkdir(exist_ok=True)

# List S3 files
def s3_run(bucket, prefix):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key'], obj['LastModified'].date()

# Filter files by last processed date
filtered_files = [(key, date) for key, date in s3_run('openalex', 'data/works/') if date > last_processed_date]


# Process each file
# APPEND data to existing csv file  
# Use pandas instead of duckdb  
def process_file(file_info):
    key, _ = file_info
    filename = f"s3://openalex/{key}"
    outfile = Path(key).name.replace('.gz', '.csv')
    outpath = raw_path / outfile
    
    df = pd.read_json(filename, lines=True, chunksize=10000)
    
    for chunk in df:
        filtered_chunk = chunk[chunk['best_oa_location'].notna()]
        filtered_chunk['url'] = filtered_chunk['best_oa_location'].apply(lambda x: x.get('pdf_url') if isinstance(x, dict) else None)
        filtered_chunk = filtered_chunk[filtered_chunk['url'].notna() & (filtered_chunk['url'] != 'null')]
        
        result = filtered_chunk[['doi', 'url']]
        result.to_csv(outpath, mode='a', header=False, index=False)


# Use multiprocessing to process files
with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    executor.map(process_file, filtered_files)


# Create output (brick) directory and load data into parquet
# If parquet file exists, append data to it. Otherwise, create new parquet file.
# UNION ALL combines data from existing parquet file and new csv files.
out = Path('brick')
out.mkdir(exist_ok=True)
parquet_file = out / 'open_alex_open_acccess_pdfs.parquet'

# Reading existing parquet file
existing_df = pd.read_parquet(parquet_file, index=False, engine='fastparquet') if parquet_file.exists() else pd.DataFrame(columns=['doi', 'url'])


# Combine data from new csv files   
new_data = pd.concat([pd.read_csv(f, names=['doi', 'url']) for f in raw_path.glob('*.csv')], ignore_index=True)

# Combine existing and new data
combined_df = pd.concat([existing_df, new_data], ignore_index=True)

# Remove duplicates and save to parquet
combined_df.drop_duplicates(subset=['doi'], keep='last').to_parquet(parquet_file, index=False, engine='fastparquet')

# Update the last processed date
if filtered_files:
    last_processed_date = max(date for _, date in filtered_files)
    save_last_processed_date(last_processed_date)

print(f"Processed files up to: {last_processed_date}")



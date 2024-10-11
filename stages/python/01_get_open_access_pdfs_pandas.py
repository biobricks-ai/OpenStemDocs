import sys
import glob
from pathlib import Path
import subprocess
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
# If there is no last processed date, use the date of one month before today. 
# return datetime object instead of date object
def get_last_processed_date():
    last_processed_file = Path('last_processed_date.txt')
    if last_processed_file.exists():
        with open(last_processed_file, 'r') as file:
            return datetime.strptime(file.read(), "%Y-%m-%d")
    else:
        print("No last processed date found, using 120 days before today.")
        return date.today() - timedelta(days=120)

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
#    df['url'] = df['best_oa_location'].apply(lambda x: x.get('pdf_url') if isinstance(x, dict) else None)
#    df['publication_date'] = pd.to_datetime(df['publication_date'])

#    df['url'] = df['url'].str.split(',').str[0].str.strip()
    
#    filtered_df = df[(df['url'].notna()) & (df['url'] != 'null') & 
#        (df['publication_date'] > pd.to_datetime(last_processed_date)) &
#        (df['doi'].str.contains('doi.org', case=False, na=False))][['doi', 'url', 'publication_date']]
    
    # Append the results to the CSV file
#    filtered_df.to_csv(outpath, mode='a', header=False, index=False)


    for chunk in df:
        chunk['publication_date'] = pd.to_datetime(chunk['publication_date'])
        chunk['url'] = chunk['best_oa_location'].apply(lambda x: x.get('pdf_url') if isinstance(x, dict) else None)

        chunk['url'] = chunk['url'].str.split(',').str[0].str.strip()

        filtered_chunk = chunk[(chunk['url'].notna()) & (chunk['url'] != 'null') & 
            (chunk['publication_date'] > pd.to_datetime(last_processed_date)) &
            (chunk['doi'].str.contains('doi.org', case=False, na=False))][['doi', 'url', 'publication_date']]
        
        result = filtered_chunk[['doi', 'url', 'publication_date']]
        result.to_csv(outpath, mode='a', header=False, index=False)



# Use multiprocessing to process files
with ProcessPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    executor.map(process_file, filtered_files)


# Create output (brick) directory and load data into parquet
# If parquet file exists, append data to it. Otherwise, create new parquet file.
# UNION ALL combines data from existing parquet file and new csv files.
out = Path('brick')
out.mkdir(exist_ok=True)
parquet_file = out / 'open_alex_open_acccess_pdfs.parquet'

# Reading existing parquet file
existing_df = pd.read_parquet(parquet_file, engine='fastparquet') if parquet_file.exists() else pd.DataFrame(columns=['doi', 'url', 'publication_date'])


# Combine data from new csv files   
#new_data = pd.concat([pd.read_csv(file, names=['doi', 'url', 'publication_date']) for file in raw_path.glob('*.csv')], ignore_index=True)
new_data = pd.DataFrame()
for file in raw_path.glob('*.csv'):
    chunk = pd.read_csv(file, names=['doi', 'url', 'publication_date'])
    new_data = pd.concat([new_data, chunk], ignore_index=True)


# Combine existing and new data
combined_df = pd.concat([existing_df, new_data], ignore_index=True)
combined_df['publication_date'] = pd.to_datetime(combined_df['publication_date'])
combined_df = combined_df.sort_values('publication_date')
# Remove duplicates and save to parquet
combined_df.drop_duplicates(subset=['doi'], keep='last')
combined_df.to_parquet(parquet_file, index=False, engine='fastparquet')



# Update the last processed date
last_processed_date = combined_df['publication_date'].max()
if last_processed_date:
    save_last_processed_date(last_processed_date)

print(f"Processed files up to: {last_processed_date} (publication date)")




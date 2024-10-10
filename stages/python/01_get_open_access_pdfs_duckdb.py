import sys
import glob
from pathlib import Path
import subprocess
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import duckdb
import json
import boto3
from botocore import UNSIGNED
from botocore.client import Config

# Get last processed date  
# If there is no last processed date, use the date of one month before today. 
def get_last_processed_date():
    last_processed_file = Path('last_processed_date.txt')
    if last_processed_file.exists():
        with open(last_processed_file, 'r') as file:
            return datetime.strptime(file.read().strip(), "%Y-%m-%d")
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
filtered_files = [
    (key, date) for key, date in s3_run('openalex', 'data/works/')
    if date > last_processed_date
]


# Process each file
# APPEND data to existing csv file  
def process_file(file_info):
    key, _ = file_info
    filename = f"s3://openalex/{key}"
    outfile = Path(key).name.replace('.gz', '.csv')
    outpath = raw_path / outfile
    
    query = f"""
    copy (
        select doi, 
               json_extract(best_oa_location, '$.pdf_url') as url,
               cast(publication_date as timestamp) as publication_date
        from read_json('{filename}', ignore_errors=true, maximum_object_size=100000000) 
        where json_extract(best_oa_location, '$.pdf_url') is not null 
          and json_extract(best_oa_location, '$.pdf_url') != 'null'
          and cast(publication_date as timestamp) > '{last_processed_date}'
    )
    to '{outpath}' (HEADER false, APPEND)
    """
    
    with duckdb.connect(':memory:') as conn:
        conn.execute(query)


# Use multiprocessing to process files
with ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
    executor.map(process_file, filtered_files)


# Create output (brick) directory and load data into parquet
# If parquet file exists, append data to it. Otherwise, create new parquet file.
# UNION ALL combines data from existing parquet file and new csv files.
out = Path('brick')
out.mkdir(exist_ok=True)
conn = duckdb.connect(':memory:')
parquet_file = out / 'open_alex_open_acccess_pdfs.parquet'

if parquet_file.exists():
    # Read existing parquet file
    conn.execute(f"create table existing as select * from parquet_scan('{parquet_file}')")

    # Combine existing and new data
    conn.execute(f"""
    create table combined as
    select * from existing
    union all
    select doi, url, cast(publication_date as timestamp) as publication_date
    from read_csv_auto('{raw_path}/*.csv', header=false, names=['doi', 'url', 'publication_date'])
    """)
else:
    # Create combined table from new data only
    conn.execute(f"""
    create table combined as
    select doi, url, cast(publication_date as timestamp) as publication_date
    from read_csv_auto('{raw_path}/*.csv', header=false, names=['doi', 'url', 'publication_date'])
    """)

# Remove duplicates, sort by publication_date, and save to parquet
conn.execute(f"""
copy (
    select distinct on (doi) *
    from combined
    order by doi, publication_date desc
)
to '{parquet_file}' (format parquet)
""")

# Update the last processed date
conn.execute(f"select max(publication_date) as max_date from '{parquet_file}'")
last_processed_date = conn.fetchone()[0]

if last_processed_date:
    save_last_processed_date(last_processed_date)

conn.close()

print(f"Processed files up to: {last_processed_date} (publication date)")



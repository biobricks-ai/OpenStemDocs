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
import shutil


#####Functions #####

# Get last processed date if applicable
def get_last_processed_date():
    last_processed_file = Path('last_processed_date.txt')
    if last_processed_file.exists():
        with open(last_processed_file, 'r') as file:
            return datetime.strptime(file.read().strip(), "%Y-%m-%d").date()
    else:
        print("No last processed date found, using 324 years before today starting from Jan 1 (testing for now)")
        today = date.today()
        start_date = date(today.year - 324, 1, 1)
        return start_date

# Connect to AWS s3 and retrieve data
def s3_run(bucket, prefix):
    s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))
    paginator = s3.get_paginator('list_objects_v2')

    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get('Contents', []):
            yield obj['Key'], obj['LastModified'].date()

# Extract url and process each file 
def process_file(file_info):
    key, _ = file_info
    filename = f"s3://openalex/{key}"

    hash_value = hashlib.md5(key.encode()).hexdigest()
    group = int(hash_value, 16) % numfile 

    outfile = f"url_{group:02d}.parquet"
    outpath = raw_path / outfile
    
    df = pd.read_json(filename, lines=True, chunksize=1000000)

    for chunk in df:
        chunk['publication_date'] = pd.to_datetime(chunk['publication_date'])
        chunk['url'] = chunk['best_oa_location'].apply(lambda x: x.get('pdf_url') if isinstance(x, dict) else None)
        chunk['url'] = chunk['url'].str.split(',').str[0].str.strip()
        
        # if publication is open access
        chunk['is_oa'] = chunk['open_access'].apply(lambda x: x.get('is_oa') if isinstance(x, dict) else None)
        
        # extract journal name, authors, topic areas and themes, and keywords
        chunk['authors'] = chunk['authorships'].apply(lambda x: ', '.join([author['author']['display_name'] for author in x]) if isinstance(x, list) else None)
        chunk['themes'] = chunk['topics'].apply(lambda x: ', '.join([topic['display_name'] for topic in x]) if isinstance(x, list) else None)
        chunk['keywd'] = chunk['keywords'].apply(lambda x: ', '.join([keyword['display_name'] for keyword in x]) if isinstance(x, list) else None)
        chunk['areas'] = chunk['topics'].apply(lambda x: ', '.join(topic['field']['display_name'] for topic in x if isinstance(topic, dict) and 'field' in topic and 'display_name' in topic['field']) if isinstance(x, list) else None)

        # extract volume and issue
        for col in ['volume', 'issue']:
            chunk[col] = chunk['biblio'].apply(lambda x: x.get(col) if isinstance(x, dict) else None)

        # metadata features
        selected_columns = [
            'id', 'doi', 'url','type', 'type_crossref', 'publication_date', 'title', 
            'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language'
        ]

        # filtering condition
        filtered_chunk = chunk[(chunk['url'].notna()) & (chunk['url'] != 'null') & 
            (chunk['publication_date'] >= pd.to_datetime(last_processed_date)) &
            (chunk['doi'].str.contains('doi.org', case=False, na=False)) &
            (chunk['title'].notna()) & (chunk['title'] != 'null') &
            (chunk['authors'].notna()) & (chunk['authors'] != 'null') &
            (chunk['is_retracted'] == False) & (chunk['is_paratext'] == False)][selected_columns]

        filtered_chunk = filtered_chunk.drop_duplicates(subset='doi', keep='first')

        # handling issues with corrupted files and cases when to add new data
        temp_outpath = outpath.with_suffix('.tmp')  
        if outpath.exists():
            try:
                existing_data = pd.read_parquet(outpath)
            except Exception:
                outpath.unlink()
                existing_data = pd.DataFrame()
            combined_data = pd.concat([existing_data, filtered_chunk]).drop_duplicates(subset='doi', keep='first')
        else:
            combined_data = filtered_chunk

        combined_data.to_parquet(temp_outpath, engine='fastparquet', compression='snappy', index=False)

        if temp_outpath.exists():
            temp_outpath.rename(outpath) 


# Check the output parquet files and removed duplicates
def remove_duplicates(parquet_dir):
    parquet_dir = Path(parquet_dir)
    parquet_files = parquet_dir.glob('*.parquet')

    seen_dois = set()

    # Iterate through all parquet files
    for file in parquet_files:
        df = pd.read_parquet(file)

        # Check for duplicates 
        unique_dois = []

        for doi in df['doi']:
            if doi not in seen_dois:
                unique_dois.append(doi)
                seen_dois.add(doi)
        
        df_unique = df[df['doi'].isin(unique_dois)]

        # drop duplicates 
        df_drop = df_unique.drop_duplicates(subset='doi', keep='first')

        df_drop.to_parquet(file, index=False)

    print("\nDuplicate doi have been removed.")


##### Execution #####

# retrieve last processed date or its arbitrary date
last_processed_date = get_last_processed_date()
print(last_processed_date)

# Create directory for the processed data
raw_path = Path('brick/articles.parquet')
raw_path.mkdir(exist_ok=True)

# Number of output files to split into (can be more or less)
numfile = 8

# Filter files by last processed date
filtered_files = [(key, date) for key, date in s3_run('openalex', 'data/works/') if date >= last_processed_date]


# Process files using multiprocessors
with ProcessPoolExecutor(max_workers=numfile) as executor:
    executor.map(process_file, filtered_files)


# Calculate the new processed date
all_parquet_files = list(raw_path.glob('*.parquet'))
if all_parquet_files:
    df_list = [pd.read_parquet(file) for file in all_parquet_files]
    combined_df = pd.concat(df_list, ignore_index=True)
    new_processed_date = combined_df['publication_date'].max().date()

    with open('last_processed_date.txt', 'w') as file:
        file.write(new_processed_date.strftime("%Y-%m-%d"))

    print(f"Processed publications from {last_processed_date} to: {new_processed_date}")
else:
    print("No new data processed.")
    new_processed_date = last_processed_date


# Check the output parquet files and remove duplicates
remove_duplicates(raw_path)

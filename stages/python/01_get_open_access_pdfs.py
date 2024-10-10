import sys
import glob
from pathlib import Path
import subprocess
import multiprocessing
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
import duckdb

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
s3_run = f"aws s3 ls --recursive --no-sign-request s3://openalex/data/works/"
files = subprocess.check_output(s3_run.split(), text=True).splitlines()

# Filter files by last processed date
filtered_files = [
    file for file in files 
    if len(file.split()) >= 2 
    and datetime.strptime(file.split()[0], "%Y-%m-%d").date() > last_processed_date
]

# Process each file
def process_file(file):
    filename = f"s3://openalex/{file.split()[-1]}"
    outfile = Path(file.split()[-1]).with_suffix('.csv').name
    outpath = raw_path / outfile
    
    query = f"""
    copy (
        select doi, best_oa_location->'$.pdf_url' as url 
        from read_json('{filename}', ignore_errors=true, maximum_object_size=100000000) 
        where url is not null and url != 'null' and not url like '%null'
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
out = Path('brick')
out.mkdir(exist_ok=True)
conn = duckdb.connect(':memory:')
parquet_file = out / 'open_alex_open_acccess_pdfs.parquet'
if parquet_file.exists():
    conn.execute(f"""
    copy (
        select * from parquet_scan('{parquet_file}')
        union all
        select * from read_csv_auto('{raw_path}/*.csv', union_by_name=true)
    ) to '{parquet_file}' (format parquet, overwrite_or_ignore)
    """)
else:
    conn.execute(f"copy (select * from read_csv_auto('{raw_path}/*.csv', union_by_name=true)) to '{parquet_file}' (format parquet)")

conn.close()

# Update the last processed date
if filtered_files:
    last_processed_date = max(datetime.strptime(file.split()[0], "%Y-%m-%d").date() for file in filtered_files)

    save_last_processed_date(last_processed_date)

print(f"Processed files up to: {last_processed_date}")



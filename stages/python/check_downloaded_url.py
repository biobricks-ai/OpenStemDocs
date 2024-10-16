import pandas as pd
from pathlib import Path
import fastparquet
from collections import defaultdict
import argparse

# Go through individual parquet files containing retrieved doi and url
# Get total number of publications from the oldest to the latest date
# Check if there are duplicates within files or across files      

parser = argparse.ArgumentParser(description="Checking duplicate DOIs")
parser.add_argument("parquet_dir", help="Path to parquet files")
args = parser.parse_args()


def check_downloaded_url(parquet_dir):
    parquet_dir = Path(parquet_dir)
    parquet_files = parquet_dir.glob('*.parquet')

    oldest_date = None
    oldest_doi = None
    latest_date = None
    latest_doi = None
    

    all_dois = defaultdict(list)

    duplicates_per_file = {}
    total_duplicates_within_files = 0
    total_duplicates_across_files = 0

    publications_per_file = {}
    total_files = 0

    for file in parquet_files:
        df = pd.read_parquet(file)

        total_files += len(df)

        total_publications = len(df)
        publications_per_file[file.name] = total_publications
            
        df['publication_date'] = pd.to_datetime(df['publication_date'])

        file_oldest_date = df['publication_date'].min()
        file_latest_date = df['publication_date'].max()

        if oldest_date is None or file_oldest_date < oldest_date:
            oldest_date = file_oldest_date
            oldest_row = df.loc[df['publication_date'].idxmin()]
            oldest_doi = oldest_row['doi']

        if latest_date is None or file_latest_date > latest_date:
            latest_date = file_latest_date
            latest_row = df.loc[df['publication_date'].idxmax()]
            latest_doi = latest_row['doi']
        
        # Check for duplicates 
        duplicates_in_file = df[df['doi'].duplicated(keep=False)]
        duplicate_count = len(duplicates_in_file)
        duplicates_per_file[file.name] = duplicate_count
        total_duplicates_within_files += duplicate_count        

        for doi in df['doi']:
            all_dois[doi].append(file.name)

    # Count duplicates across files
    for doi, files in all_dois.items():
        if len(files) > 1:
            total_duplicates_across_files += 1
    
    
    print(f"\nTotal publications from {oldest_date} to {latest_date} is {total_files}.")
    print(f"Oldest DOI: {oldest_doi}")
    print(f"Latest DOI: {latest_doi}")

    print(f"\nTotal duplicates within files: {total_duplicates_within_files}")
    print(f"Total duplicates across files: {total_duplicates_across_files}")
    print(f"Total duplicates: {total_duplicates_within_files + total_duplicates_across_files}")

    print("\nNumber of duplicates in each output file:")
    for (file, duplicates), (_, total_pub) in sorted(zip(duplicates_per_file.items(), publications_per_file.items()), key=lambda x: x[1][1], reverse=True):
        print(f"{file}: {duplicates} out of {total_pub}")


check_downloaded_url(args.parquet_dir)
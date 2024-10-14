import pandas as pd
from pathlib import Path
import fastparquet
from collections import defaultdict
import argparse

# Get total number of publications from the oldest to the latest date
# Check for duplicates within files and across files    

parser = argparse.ArgumentParser(description="Checking duplicate DOIs")
parser.add_argument("parquet_dir", help="Path to parquet files")
args = parser.parse_args()


def main(parquet_dir):
    parquet_dir = Path(parquet_dir)
    parquet_files = parquet_dir.glob('*.parquet')

    oldest_date = None
    latest_date = None

    all_dois = defaultdict(list)

    duplicates_per_file = {}
    total_duplicates_within_files = 0
    total_duplicates_across_files = 0

    publications_per_file = {}
    total_files = 0

    # Iterate through all parquet files
    for file in parquet_files:
        df = pd.read_parquet(file)

        total_files += len(df)

        total_publications = len(df)
        publications_per_file[file.name] = total_publications
            
        df['publication_date'] = pd.to_datetime(df['publication_date'])

        file_oldest_date = df['publication_date'].min()
        file_latest_date = df['publication_date'].max()

        if oldest_date is None:
            oldest_date = file_oldest_date.strftime("%Y-%m-%d")

        if latest_date is None:
            latest_date = file_latest_date.strftime("%Y-%m-%d")

        # Check for duplicates within the current file

        duplicates_in_file = df[df['doi'].duplicated(keep=False)]
        duplicate_count = len(duplicates_in_file)
        duplicates_per_file[file.name] = duplicate_count
        total_duplicates_within_files += duplicate_count        

        #if not duplicates_in_file.empty:
        #    duplicate_count = len(duplicates_in_file)
        #    total_duplicates_within_files += duplicate_count
            #print(f"Duplicates found within file {file.name}: {duplicate_count}")
            #print(duplicates_in_file['doi'].value_counts())

        # Check for duplicates across files
        for doi in df['doi']:
            all_dois[doi].append(file.name)

    # Count duplicates across files
    for doi, files in all_dois.items():
        if len(files) > 1:
            total_duplicates_across_files += 1
            #print(f"Duplicate DOI found across files: {doi}")
            #print(f"  Present in: {', '.join(files)}")

    
    # drop duplicates within files and update files
        df_drop = df.drop_duplicates(subset='doi', keep='first')

        total_now += len(df_drop)

        for doi in df_drop['doi']:
            all_dois[doi].append(file.name)

        df_drop.to_parquet(file, index=False)
    
    
    print(f"Total publications from {oldest_date} to {latest_date} is {total_files}.")
    print(f"Total duplicates within files: {total_duplicates_within_files}")
    print(f"Total duplicates across files: {total_duplicates_across_files}")
    print(f"Total duplicates: {total_duplicates_within_files + total_duplicates_across_files}")

    print("\nNumber of duplicates in each output file:")
    for (file, duplicates), (_, total_pub) in sorted(zip(duplicates_per_file.items(), publications_per_file.items()), key=lambda x: x[1][1], reverse=True):
        print(f"{file}: {duplicates} out of {total_pub}")


    print("\nRemoving duplicates within file and updating files")
    print(f"Total publications after dropping duplicates within files: {total_now}")


main(args.parquet_dir)
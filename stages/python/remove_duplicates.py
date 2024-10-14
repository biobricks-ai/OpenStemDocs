import pandas as pd
from pathlib import Path
import fastparquet
from collections import defaultdict
import argparse


parser = argparse.ArgumentParser(description="Checking duplicate DOIs")
parser.add_argument("parquet_dir", help="Path to parquet files")
args = parser.parse_args()


def main(parquet_dir):
    parquet_dir = Path(parquet_dir)
    parquet_files = parquet_dir.glob('*.parquet')


    all_dois = defaultdict(list)
    seen_dois = set()

    duplicates_per_file = {}
    total_duplicates_within_files = 0
    total_duplicates_across_files = 0

    # Iterate through all parquet files
    for file in parquet_files:
        df = pd.read_parquet(file)

        # Check for duplicates 

        duplicates_in_file = df[df['doi'].duplicated(keep=False)]
        duplicate_count = len(duplicates_in_file)
        duplicates_per_file[file.name] = duplicate_count
        total_duplicates_within_files += duplicate_count        

        unique_dois = []

        for doi in df['doi']:
            if doi not in seen_dois:
                unique_dois.append(doi)
                seen_dois.add(doi)
        
        df_unique = df[df['doi'].isin(unique_dois)]

        # drop duplicates 
        df_drop = df_unique.drop_duplicates(subset='doi', keep='first')

        total_now += len(df_drop)

        df_drop.to_parquet(file, index=False)

    total_duplicates_across_files = len(seen_dois) - len(set(df['doi'] for df in parquet_files))


    print("\nRemoved duplicates within and across files")


main(args.parquet_dir)
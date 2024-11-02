import hashlib
import os
import dotenv
import requests
import pandas as pd
import fastparquet
from fastparquet.writer import write
from pathlib import Path
import pypdf
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

dotenv.load_dotenv()
scraperapi_key = os.getenv("SCRAPERAPI_KEY")

##### Functions #####

# download pdf from url
def download_pdf(url, file_output_dir, downloaded_hashes, session=None):
    # safely manage session when using multiple threads
    if session is None:
        session = requests.Session()
    
    # skip an url if connection error occurs (RequestsException covers ConnectionError, HTTPError and JSONError)
    try:
        response = requests.get(f"http://api.scraperapi.com?api_key={scraperapi_key}&url={url}&render=true")
        response.raise_for_status()
    except requests.exceptions.RequestException:
        return None, None 
    
    if response.status_code == 200:
        content = response.content

        if len(content) < 1024:     # too small to be a pdf  (< 1 KB)
            return None, None
        
        # check if the content type is pdf
        if 'application/pdf' in response.headers.get('content-type', '').lower() or \
            'application/octet-stream' in response.headers.get('content-type', '').lower():
            content_hash = hashlib.md5(content).hexdigest()

            if not content.startswith(b'%PDF-'):
                return None, None

            # save pdf (note that pdf is a binary file)
            outfile_path = file_output_dir / f"{content_hash}.pdf"
            file_output_dir.mkdir(parents=True, exist_ok=True)   
            with outfile_path.open('wb') as file:
                file.write(content)

            # check validity of pdf (Exception covers PdfReadError and other PyPDF errors)
            try:
                with outfile_path.open('rb') as file:
                    reader = pypdf.PdfReader(file)
                    if len(reader.pages) > 0:
                        return outfile_path, content_hash
            except Exception:
                if outfile_path.exists(): 
                    outfile_path.unlink() 
                return None, None
                
            if outfile_path.exists():
                outfile_path.unlink()
    
    return None, None

##### Execution #####

# set input and output directories
input_dir = Path('brick/articles.parquet')
output_dir = Path('/mnt/ssd_raid/workspace-paween/projects/OpenStemDocs/brick/pdfs')
output_dir.mkdir(parents=True, exist_ok=True)
metadata_output_dir = Path('/mnt/ssd_raid/workspace-paween/projects/OpenStemDocs/brick/downloads.parquet')
metadata_output_dir.mkdir(parents=True, exist_ok=True)

# define final metadata columns or descriptors
metadata_columns = [
    'id', 'doi', 'url', 'type', 'type_crossref', 'publication_date',
    'title', 'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language',
    'content_hash', 'file_path'
] 

# process each file
for file in input_dir.glob('*.parquet'):
    df = pd.read_parquet(file)[:150000]

    file_stem = file.stem

    # create output directory (url_00, url_01, ...) for downloaded pdfs
    file_output_dir = output_dir / file_stem
    file_output_dir.mkdir(parents=True, exist_ok=True)

    # Get the latest row where content_hash is assigned (start from where it is left off instead of starting from row 0)
    metadata_file = metadata_output_dir / f"{file_stem}_pdfs.parquet"
    existing_metadata = pd.read_parquet(metadata_file) if metadata_file.exists() else pd.DataFrame(columns=['content_hash'])
        
    latest_row = existing_metadata[existing_metadata['content_hash'].notnull()].iloc[-1] if not existing_metadata.empty else None

    if latest_row is not None and 'doi' in latest_row:
        start_index = df[df['doi'] == latest_row['doi']].index[0] + 1 if not df[df['doi'] == latest_row['doi']].empty else 0
    else:
        start_index = 0

    dois = df['doi'].iloc[start_index:].tolist() 
    urls = df['url'].iloc[start_index:].tolist()

    downloaded_hashes = set(existing_metadata['content_hash'].tolist())  

    # Execute download using multiple threads (I/O bound operation)
    results_list = []
    with ThreadPoolExecutor(max_workers=40) as executor:
        results = list(tqdm(executor.map(
            lambda doi_url: (
                doi_url[1], 
                doi_url[0], 
                *download_pdf(doi_url[1], file_output_dir, downloaded_hashes)
            ),
            zip(dois, urls)
        ), total=len(dois), desc='Downloading PDFs'))

    for result in results:
        if result and result[2] not in downloaded_hashes:
            if result[2] is not None and result[3] is not None:
                results_list.append({
                    'doi': result[1],                # DOI  
                    'url': result[0],                # URL
                    'content_hash': str(result[3]),  # Hash
                    'file_path': str(result[2])      # Path to pdf file
                })
                downloaded_hashes.add(result[2])

    results_df = pd.DataFrame(results_list).drop_duplicates(subset=['doi','content_hash'])


    # merge hash and pdf path to original data
    # append or add updates or new data to existing file 
    if not results_df.empty:

        # descriptors in the input parquet file        
        original_columns = [
            'id', 'doi','type', 'type_crossref', 'publication_date', 'title', 
            'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language'
            ]
    
        original_df = df[original_columns]

        metadata_df = pd.merge(results_df, original_df, on='doi', how='left')
        metadata_df = metadata_df.dropna(subset=['content_hash', 'file_path'])

        metadata_df = metadata_df[metadata_columns]
        metadata_df = metadata_df.drop_duplicates(subset=['doi', 'content_hash'])


        output_file = metadata_output_dir / f"{file_stem}_pdfs.parquet"
        if output_file.exists():
            existing_df = pd.read_parquet(output_file)
            combined_df = pd.concat([existing_df, metadata_df], ignore_index=True).drop_duplicates(subset=['doi', 'content_hash'])
            combined_df.to_parquet(output_file, index=False)
        else:
            metadata_df.to_parquet(output_file, index=False)




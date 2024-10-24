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
def download_pdf(url, file_output_dir, downloaded_hashes):
    response = requests.get(f"http://api.scraperapi.com?api_key={scraperapi_key}&url={url}&render=true")
        
    if response.status_code == 200 and (
        'application/pdf' in response.headers.get('content-type', '').lower() or
        'application/octet-stream' in response.headers.get('content-type', '').lower()):
        content = response.content
        content_hash = hashlib.md5(content).hexdigest()
            
        if content_hash in downloaded_hashes:
            return None, None

        # get rid of too small files
        if len(content) < 1024:
            return None, None

        filename = f"{content_hash}.pdf"
        outfile_path = file_output_dir / filename

        file_output_dir.mkdir(parents=True, exist_ok=True)
        
        with outfile_path.open('wb') as file:
            file.write(content)

        # check if the file is a valid PDF
        if content.startswith(b'%PDF-'):
            try:
                with outfile_path.open('rb') as file:
                    reader = pypdf.PdfReader(file)
                    if len(reader.pages) > 1:
                        return outfile_path, content_hash
            except pypdf.errors.PdfReadError:
                outfile_path.unlink()
                return None, None
            except Exception as e:
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
    # read parquet file (can be adjusted to read only a subset of the file)
    df = pd.read_parquet(file)[:5000]

    file_stem = file.stem

    # create output directory (url_00, url_01, ...) for downloaded pdfs
    file_output_dir = output_dir / file_stem
    file_output_dir.mkdir(parents=True, exist_ok=True)

    # Get the latest row where content_hash is assigned
    # Start from where it is left off (last downloaded pdf) instead of starting from row 0
    metadata_file = metadata_output_dir / f"{file_stem}_pdfs.parquet"
    existing_metadata = pd.read_parquet(metadata_file) if metadata_file.exists() else pd.DataFrame(columns=['content_hash'])
        
    latest_row = existing_metadata[existing_metadata['content_hash'].notnull()].iloc[-1] if not existing_metadata.empty else None

    if latest_row is not None and 'doi' in latest_row:
        start_index = df[df['doi'] == latest_row['doi']].index[0] if not df[df['doi'] == latest_row['doi']].empty else 0
    else:
        start_index = 0

    dois = df['doi'].tolist()[start_index:] 
    urls = df['url'].tolist()[start_index:]

    downloaded_hashes = set(existing_metadata['content_hash'].tolist())  

    # Execute download and metadata extraction in parallel
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
                    'file_path': str(result[2])     # Path
                })
                downloaded_hashes.add(result[2])

    # ensure no duplicates  
    results_df = pd.DataFrame(results_list).drop_duplicates(subset=['doi','content_hash'])


    # merge hash, pdf path, journal and publisher to original data
    # handle issues when no new data is generated
    if not results_df.empty:

        # descriptors in the input parquet file        
        original_columns = [
            'id', 'doi','type', 'type_crossref', 'publication_date', 'title', 
            'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language'
            ]
    
        original_df = df[original_columns]

        metadata_df = pd.merge(results_df, original_df, on='doi', how='left')

        # remove rows with no downloaded pdfs
        metadata_df = metadata_df.dropna(subset=['content_hash', 'file_path'])

        # select columns and remove duplicates
        metadata_df = metadata_df[metadata_columns]
        metadata_df = metadata_df.drop_duplicates(subset=['doi'])


        output_file = metadata_output_dir / f"{file_stem}_pdfs.parquet"
        # add updated data to existing file and remove duplicates
        if output_file.exists():
            existing_df = pd.read_parquet(output_file)
            combined_df = pd.concat([existing_df, metadata_df], ignore_index=True).drop_duplicates(subset=['doi', 'content_hash'])
            combined_df.to_parquet(output_file, index=False)
        else:
            metadata_df.to_parquet(output_file, index=False)




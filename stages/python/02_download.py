import hashlib
import requests
import pandas as pd
import fastparquet
from pathlib import Path
import pypdf
from concurrent.futures import ThreadPoolExecutor
from tqdm import tqdm

scraperapi_key = "ENTER API KEY"

##### Functions #####

# download pdf from url
def download_pdf(url, output_dir):
    params = {'api_key': scraperapi_key, 'url': url}
    response = requests.get(f"http://api.scraperapi.com?api_key={scraperapi_key}&url={url}&render=true")
    if response.status_code == 200 and (
        'application/pdf' in response.headers.get('content-type', '').lower() or
        'application/octet-stream' in response.headers.get('content-type', '').lower()):
        content = response.content
        content_hash = hashlib.md5(content).hexdigest()
        filename = f"{content_hash}.pdf"
        outfile_path = output_dir / filename
        
        with outfile_path.open('wb') as file:
            file.write(content)

        if content.startswith(b'%PDF-'):            
            with outfile_path.open('rb') as file:
                reader = pypdf.PdfReader(file)
                if len(reader.pages) > 0:
                    return outfile_path, content_hash
   
        outfile_path.unlink()
    
    return None, None

# retrieve publisher and journal from crossref
def extract_metadata(doi):
    base_url = "https://api.crossref.org/works/"
    response = requests.get(f"http://api.scraperapi.com",params={"api_key":scraperapi_key,"url":f"{base_url}{doi}"})
    if response.status_code == 200:
        data = response.json()['message']
        journal = data.get('container-title', [])
        journal = journal[0] if journal else None
        publisher = data.get('publisher', [])
        publisher = publisher if publisher else None            
        return {'journal': journal, 'publisher': publisher}
    else:
        return {'journal': None, 'publisher': None}

##### Execution #####

# input and output directories
input_dir = Path('brick')
output_dir = Path('brick/pdfs')
output_dir.mkdir(parents=True, exist_ok=True)

# number of files to process
numfile = len(list(input_dir.glob('*.parquet')))

# process each file
for file in input_dir.glob('*.parquet'):
    df = pd.read_parquet(file)
    dois = df['doi'].tolist()
    urls = df['url'].tolist()

    downloaded_hashes = set()

    with ThreadPoolExecutor(max_workers=numfile) as executor:
        results = list(tqdm(executor.map(
            lambda doi_url: (
                doi_url[1], 
                doi_url[0], 
                *download_pdf(doi_url[1], output_dir), 
                *extract_metadata(doi_url[0]).values()
            ),
            zip(dois, urls)
        ), total=len(dois), desc='Downloading PDFs'))

    results_list = []
    
    for result in results:
        if result and result[2] not in downloaded_hashes: 
            results_list.append({
                'doi': result[1],                #DOI  
                'url': result[0],                #URL
                'content_hash': str(result[3]),  # Hash
                'file_path': str(result[2]),     # Path
                'journal': str(result[4]),       # Journal
                'publisher': str(result[5])      # Publisher
            })
            downloaded_hashes.add(result[2])

    
    results_df = pd.DataFrame(results_list)

    # merge hash, pdf path, journal and publisher to original data
    # original columns
    original_columns = [
            'id', 'doi','type', 'type_crossref', 'publication_date', 'title', 
            'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language'
        ]
    
    original_df = df[original_columns]

    metadata_df = pd.merge(results_df, original_df, on='doi', how='left')

    # remove rows with no downloaded pdfs
    metadata_df = metadata_df.dropna(subset=['content_hash', 'file_path'])

    # arrange columns
    final_columns = [
            'id', 'doi', 'url', 'type', 'type_crossref', 'publication_date', 'journal', 'publisher', 
            'title', 'is_oa', 'authors', 'areas', 'themes', 'keywd', 'volume', 'issue', 'language',
            'content_hash', 'file_path'
        ] 

    metadata_df = metadata_df[final_columns]

    output_parquet = file.stem + '_pdfs.parquet'
    metadata_df.to_parquet(output_dir / output_parquet, index=False)


import hashlib
import requests
import pandas as pd
import fastparquet
from pathlib import Path
import PyPDF2
import re

scraperapi_key = "KEY"

def download_pdf(url, output_dir):
    response = requests.get(f"http://api.scraperapi.com?api_key={scraperapi_key}&url={url}&render=true")
    if response.status_code == 200 and response.headers.get('content-type', '').lower() == 'application/pdf':
        content = response.content
        content_hash = hashlib.md5(content).hexdigest()
        filename = f"{content_hash}.pdf"
        outfile_path = output_dir / filename
        with outfile_path.open('wb') as file:
            file.write(content)


        with outfile_path.open('rb') as file:
            reader = PyPDF2.PdfReader(file)
            if len(reader.pages) > 0:
                return outfile_path, content_hash

        outfile_path.unlink()
    
    return None, None


def extract_metadata(doi):
    base_url = "https://api.crossref.org/works/"
    response = requests.get(f"http://api.scraperapi.com",params={"api_key":scraperapi_key,"url":f"{base_url}{doi}"})
    if response.status_code == 200:
        data = response.json()['message']
        title = data.get('title', [None])[0]
        journal = data.get('container-title', [None])[0]
        authors = data.get('author', [])
        all_authors = ', '.join([f"{author['given']} {author['family']}" for author in authors]) if authors else None
        return {'title': title, 'journal': journal, 'author': all_authorsauthor}
    return {'title': None, 'journal': None, 'author': None}    

input_dir = Path('brick')
output_dir = Path('brick/pdf')
output_dir.mkdir(parents=True, exist_ok=True)

for file in input_dir.glob('*.parquet'):
    df = pd.read_parquet(file)
    urls = df['url'].tolist()
    dois = df['doi'].tolist()
    publication_date = df['publication_date'].tolist()

    metadata_list = []
    downloaded_hashes = set()

    for url, doi, pub in zip(urls, dois, publication_date):
        file_path, content_hash = download_pdf(url, output_dir)
        if file_path and content_hash not in downloaded_hashes:
            metadata = extract_metadata(doi)
            year = pd.to_datetime(pub).year if pd.notna(pub) else None
            metadata_list.append({'doi': doi, 'url': url, 'file_path': str(file_path), 'content_hash': content_hash, 'year': year, **metadata})
            downloaded_hashes.add(content_hash)


    metadata_df = pd.DataFrame(metadata_list)
    output_parquet = file.stem + '_pdfs.parquet'
    metadata_df.to_parquet(Path('brick/pdf') / output_parquet, index=False)

import hashlib
import requests
import pandas as pd
from pathlib import Path
import PyPDF2
import re

scraperapi_key = "KEY"

def download_pdf(url, output_dir):
    #response = requests.get(url)
    response = requests.get(f"http://api.scraperapi.com?api_key={scraperapi_key}&url={url}&render=true")
    if response.status_code == 200 and response.headers.get('content-type', '').lower() == 'application/pdf':
        content = response.content
        content_hash = hashlib.md5(content).hexdigest()
        filename = f"{content_hash}.pdf"
        file_path = output_dir / filename
        with file_path.open('wb') as f:
            f.write(content)


        with file_path.open('rb') as f:
            reader = PyPDF2.PdfReader(f)
            if len(reader.pages) > 0:
                return file_path, content_hash

        file_path.unlink()
    
    return None, None


def extract_metadata(doi):
    base_url = "https://api.crossref.org/works/"
    #response = requests.get(f"{base_url}{doi}")
    response = requests.get(f"http://api.scraperapi.com",params={"api_key":scraperapi_key,"url":f"{base_url}{doi}"})
    if response.status_code == 200:
        data = response.json()['message']
        title = data.get('title', [None])[0]
        journal = data.get('container-title', [None])[0]
        authors = data.get('author', [])
        author = f"{authors[0]['given']} {authors[0]['family']}" if authors else None
        return {'title': title, 'journal': journal, 'author': author}
    return {'title': None, 'journal': None, 'author': None}    

input_file = Path('test/test.parquet')
output_dir = Path('test/pdf')
output_dir.mkdir(parents=True, exist_ok=True)
output_parquet = Path('test/test_pdfs.parquet')

df = pd.read_parquet(input_file)
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
metadata_df.to_parquet(output_parquet, index=False)

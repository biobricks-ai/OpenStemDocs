import hashlib
import requests
import pandas as pd
import fastparquet
from pathlib import Path
import PyPDF2
import re
from concurrent.futures import ThreadPoolExecutor

scraperapi_key = "ENTER KEY"

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
        title = data.get('title', [None])
        title = title[0] if isinstance(title, list) and title else None
        journal = data.get('container-title', [])
        journal = journal[0] if journal else None
        authors = data.get('author', [])
        all_authors = ', '.join([f"{author.get('given', '')} {author.get('family', '')}".strip() for author in authors]) if authors else None
        return {'title': title, 'journal': journal, 'author': all_authors}
    return {'title': None, 'journal': None, 'author': None}    

input_dir = Path('brick_v0')
output_dir = Path('brick_v0/pdf0')
output_dir.mkdir(parents=True, exist_ok=True)

numfile = len(list(input_dir.glob('*.parquet')))

for file in input_dir.glob('*.parquet'):
    df = pd.read_parquet(file)
    urls = df['url'].tolist()
    dois = df['doi'].tolist()
    publication_date = df['publication_date'].tolist()

    metadata_list = []
    downloaded_hashes = set()

    with ThreadPoolExecutor(max_workers=numfile) as executor:
        results = executor.map(
            lambda url_doi_pub: (url_doi_pub[0], url_doi_pub[1], 
                *download_pdf(url_doi_pub[0], output_dir), 
                pd.to_datetime(url_doi_pub[2]).year if pd.notna(url_doi_pub[2]) else None,
                *extract_metadata(url_doi_pub[1]).values()
            ), 
            zip(urls, dois, publication_date)
        )

    for result in results:
        if result and result[3] not in downloaded_hashes: 
            metadata_list.append({'doi': result[1], 'url': result[0], 'file_path': str(result[2]),
                'content_hash': result[3], 'year': result[4], 'title': result[5],
                'journal': result[6], 'author': result[7]
            })
            downloaded_hashes.add(result[3])


    metadata_df = pd.DataFrame(metadata_list)
    output_parquet = file.stem + '_pdfs.parquet'
    metadata_df.to_parquet(output_dir / output_parquet, index=False)

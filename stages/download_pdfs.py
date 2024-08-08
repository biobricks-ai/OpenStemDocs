import fileinput
import concurrent.futures
from more_itertools import map_except
from pypdf import PdfReader, PdfWriter
from pypdf.errors import PdfReadError
import jsonlines
import requests
from pathlib import Path
import os
import sys
import shutil

PARAMS = {
    "binary_target": "binary",
    "ultra_premium": "false",
    "api_key": os.environ.get("SCRAPERAPI_KEY")
}

def create_month_year_folder(dt):
    dt = dt.date()
    month = dt.month
    year = dt.year
    dirname = Path(f"pdfs/{year}-{month}")
    if not dirname.exists():
        dirname.mkdir()
    return dirname

def download(url):
    initial_resp = requests.get(url, stream=True, params=PARAMS)
    if initial_resp.status_code == 200:
        return (initial_resp.headers, initial_resp.raw)
    else:
        return initial_resp.headers, None
    
def save_file(url):
    (headers, stream) = download(url)
    if stream is not None:
        try:
            dirname = create_month_year_folder(pdf.metadata.creation_date)
            temp_file = dirname / headers["filename_effective"]
            with open(temp_file, "w") as f:
                shutil.copyfileobj(stream, f)
            pdf = PdfReader(temp_file)
            wtr = PdfWriter(pdf)
            wtr.write(dirname / pdf.file_name)
        except PdfReadError as e:
            print(e)
        except OSError as os:
            print(f"OS error while downloading file: {os}")
    return headers

def write_log(headers):
    with jsonlines.open("log.json") as f:
        f.write(headers)
        
def read_input(line):
    scraper_url = "http://api.scraperapi.com/?&url=" + line if line else None
    if scraper_url:
        headers = save_file(scraper_url)
        write_log(headers)
        sys.exit(0)
    else: 
        sys.exit(1)
            
if __name__ == "__main__":
    try:
        with concurrent.futures.ProcessPoolExecutor() as exec:
            input_files = fileinput.input(encoding="utf-8")
            for v in map_except(lambda x: x, exec.map(read_input, input_files), Exception):
                print(v)
            
    except Exception as e:
        print("error")
        sys.exit(1)
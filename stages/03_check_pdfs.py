import json
import os
from pypdf import PdfReader
from pathlib import Path
import requests
import shutil
import sys

def is_readable_pdf(file_path):
    try:
        with open(file_path, 'rb') as file:
            reader = PdfReader(file)
            if len(reader.pages) > 0:
                # Try to extract text from the first page
                text = reader.pages[0].extract_text()
                return len(text.strip()) > 0
    except Exception:
        return False
    return True

def handle_bad_pdfs():
    if len(sys.argv) < 2: raise Exception(f"passed arguments insufficient: {sys.argv}")
    resp = sys.argv[1]
    data = json.loads(resp)
    if data["response_code"] and data["response_code"] != 200:
        print("Error handling download. Retrying...")
        retry_resp = requests.get(data["url"], stream=True)
        if retry_resp.status_code != 200:
            print("Retry failed.")
        else:
            match retry_resp.encoding:
                case "text/plain" | "text/html":
                    print("Scraping error.")
                case "application/x-pdf":
                    try_read_f = retry_resp.headers["filename_effective"]
                    local_path = Path("pdfs/" + try_read_f)
                    with open(local_path, "wb") as f:
                        shutil.copyfileobj(retry_resp.raw, f)
                    if not is_readable_pdf(local_path):
                        os.remove(local_path)
        

good_pdfs = []
bad_pdfs = []

if __name__ == '__main__':
    pdfs = Path("pdfs")
    for file in pdfs.iterdir():
        if is_readable_pdf(file):
            good_pdfs.append(file.name)
        else:
            bad_pdfs.append(file.name)
    print("PDFs captured:", len(good_pdfs))
    print(f"Issues encountered with: {len(bad_pdfs)} files. Retrying...")
    handle_bad_pdfs()
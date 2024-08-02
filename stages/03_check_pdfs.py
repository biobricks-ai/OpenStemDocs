import json
import requests
from pypdf import PdfReader
from pathlib import Path

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
    # find a way to replace this with the actual URL...
    retry_urls = [{f: "url"} for f in bad_pdfs]
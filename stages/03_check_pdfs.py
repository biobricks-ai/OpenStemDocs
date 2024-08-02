import jsonlines
import os
from pypdf import PdfReader
from pathlib import Path
import requests
import shutil

SCRAPERAPI_PARAMS = {
    "ultra_premium": True,
    "binary_target": "binary",
    "api_key": os.environ.get("SCRAPERAPI_KEY")
}


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


def handle_bad_pdfs(data):
    if data["response_code"] and data["response_code"] != 200:
        print("Error handling download. Retrying...")
        retry_resp = requests.get(data["url"], stream=True, params=SCRAPERAPI_PARAMS)
        if retry_resp.status_code != 200:
            print("Retry failed.")
        else:
            match retry_resp.encoding:
                case "text/plain" | "text/html":
                    print("Scraping error.")
                case "application/pdf":
                    try_read_f = retry_resp.headers["filename_effective"]
                    local_path = Path("pdfs/" + try_read_f)
                    with open(local_path, "wb") as f:
                        shutil.copyfileobj(retry_resp.raw, f)
                    if not is_readable_pdf(local_path):
                        os.remove(local_path)


if __name__ == '__main__':
    with jsonlines.open("log.json", "r") as f:
        for obj in f:
            handle_bad_pdfs(obj)

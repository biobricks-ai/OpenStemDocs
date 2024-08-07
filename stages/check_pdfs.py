import json

import pandas as pd
import jsonlines
import os
from pypdf import PdfReader
from pathlib import Path
import requests
from rich.console import Console
import shutil
import sys
from urllib.parse import parse_qs, urlsplit

console = Console()

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


def get_raw_url(s):
    split_data = urlsplit(s)
    return parse_qs(split_data.query["url"])


def handle_bad_pdfs(data):
    raw_url = get_raw_url(data["url"])
    console.log(f"Retrying download from {raw_url}")
    retry_resp = requests.get(data["url"], stream=True, params=SCRAPERAPI_PARAMS)
    if retry_resp.status_code != 200:
        console.log(f"Retry of {raw_url} failed.")
        return False
    else:
        match retry_resp.encoding:
            case "text/plain" | "text/html":
                return False
            case "application/pdf":
                try_read_f = retry_resp.headers["filename_effective"]
                local_path = Path("pdfs/" + try_read_f)
                with open(local_path, "wb") as f:
                    shutil.copyfileobj(retry_resp.raw, f)
                if not is_readable_pdf(local_path):
                    os.remove(local_path)
                    return False
                return True


if __name__ == '__main__':
    try:
        output = []
        with jsonlines.open("log.json", "r") as logfile:    
            for obj in logfile:
                metadata = {"url": get_raw_url(obj["url"]), "filename": obj["filename_effective"]}
                output.append(metadata)
                if obj["response_code"] in [400, 500]:
                    if not handle_bad_pdfs(obj):
                        print("Retry failed.")
        df = pd.DataFrame.from_records(output)
        df.to_parquet("brick/docs.parquet")
    except json.JSONDecodeError:
        print("Error decoding JSON")
    except KeyError:
        print("Missing expected key in JSON data")
    except Exception:
        sys.exit(1)

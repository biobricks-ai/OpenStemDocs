#! /usr/bin/bash

set -eo pipefail

rawpath="raw"
pdfpath="pdfs"

echo "Creating output directory for PDFs at $(pwd)/$pdfpath"
if [[ ! -d $pdfpath ]]; then mkdir $pdfpath; fi

if [[ ! -f $(pwd)/"log.json" ]]; then
  touch "log.json"
fi

# get rid of triple quotes left by DuckDB
find $rawpath -type f -iname "*.csv" -exec sed -i "s/\"\"\"//g" {} \;

for file in "$rawpath"/*.csv; do
  echo "downloading PDFs from: $file"
  python3 "$(pwd)/stages/download_pdfs.py" "$file" |
  tee /dev/tty
done

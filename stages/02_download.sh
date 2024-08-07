#! /usr/bin/bash

set -euo pipefail

rawpath="raw"
pdfpath="pdfs"

echo "Creating output directory for PDFs at $(pwd)/$pdfpath"
if [[ ! -d $pdfpath ]]; then mkdir $pdfpath; fi

if [[ ! -f $(pwd)/"log.json" ]]; then
  touch "log.json"
fi

function process_response() {
  stdbuf -oL jq -c '{filename_effective, url, response_code}' >> "$(pwd)/log.json"
}

# get rid of triple quotes left by DuckDB
find $rawpath -type f -iname "*.csv" -exec sed -i "s/\"\"\"//g" {} \;

for file in "$rawpath"/*.csv; do
  echo "downloading PDFs from: $file"
  awk '{print "http://api.scraperapi.com/?&url=" $1}' "$file" |
    xargs -P 20 -I {} curl -G {} -s -d "binary_target=binary" \
    -d "ultra_premium=false" \
    -d "api_key=$SCRAPERAPI_KEY" -O -J -L --output-dir "$pdfpath" -w "%{json}\n" | process_response
done

#! /usr/bin/bash

rawpath="raw"
pdfpath="pdfs"

mkdir $pdfpath

# get rid of triple quotes left by DuckDB
find $rawpath -type f -iname "*.csv" -exec sed -i "s/\"\"\"//g" {} \;

for file in "$rawpath"/*.csv; do
    echo "downloading PDFs from: $file"
    awk '{print "http://api.scraperapi.com/?&url=" $1}' "$file" |
        xargs -P 14 -I {} curl -G {} -s -d "binary_target=binary" \
            -d "ultra_premium=true" \
            -d "api_key=$SCRAPERAPI_KEY" -O -J -L --output-dir "$pdfpath" -w "%{json}\n" |
            jq '{filename_effective, url, response_code}' |
            tee -a >(python3 "$(pwd)"/stages/03_check_pdfs.py "$@") "log.json"
done
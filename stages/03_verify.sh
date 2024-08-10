#! /usr/bin/bash

set -euxo pipefail

pdfpath="/mnt/ssd_raid/workspace-bhlieberman/pdfs"
need_retry=$(jq -f "$(pwd)/stages/filter_responses.jq" "$(pwd)/log.json")

if [[ ! $need_retry ]]; then
    echo "JQ failed to parse the logfile"
    exit 1;
fi

function mk_request() {
    curl -G "$1" -d "ultra_premium=true" \
    -d "binary_target=binary" -d "api_key=$SCRAPERAPI_KEY" \
    -s -O -J -L --output-dir "$pdfpath" -w "%{json}\n" >> "$(pwd)/retried_requests.json"
}

for url in $need_retry; do
    retry=$(mk_request "$url")
    if [[ ! $retry ]]; then
        echo "retry failed"
    fi
done

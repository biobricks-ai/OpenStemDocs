#! /usr/bin/bash

set -eo pipefail

rawpath="raw"
pdfpath="/mnt/ssd_raid/workspace-bhlieberman/OpenStemDocs/pdfs"

export pdfpath

if [[ ! -d $pdfpath ]]; then
  echo "Creating output directory for PDFs at $(pwd)/$pdfpath"
  mkdir $pdfpath
fi

if [[ ! -f ~+/log.json ]]; then
  touch "log.json"
fi

function compute_hash() {
  # computes MD5 hash of file contents to
  # determine validity
  # this pipeline finds all the files that are duplicates
  # based on their checksum, and deletes them
  bad_pdfs=$(find "$pdfpath" -type f -exec md5sum '{}' + |
    sort --parallel=14 -k 1,1 |
    uniq -w 32 -c |
    awk '$1 > 2 {print $3}' |
    xargs -I {} sh -c 'pdftk "{}" dump_data 2>/dev/null || echo "{}"')

  if [[ -n $bad_pdfs ]]; then
    echo "$bad_pdfs" >"bad_pdfs.txt"
    echo "$bad_pdfs" | xargs rm
  fi

}

# get rid of triple quotes left by DuckDB
find $rawpath -type f -iname "*.csv" -exec sed -i "s/\"\"\"//g" {} \;

for file in "$rawpath"/part_{011..030}.csv; do
  # check if the file has any URLs in it
  if [[ $(wc -l <"$file") -eq 0 ]]; then
    echo "Empty partition, skipping..."
    continue
  fi

  echo "downloading PDFs from: $file"
  # make a sub-directory for each "part_xxx" CSV block
  subdir_name=$(basename "$file" .csv)
  if [[ ! -d "$pdfpath/$subdir_name" ]]; then
    mkdir -p "$pdfpath/$subdir_name"
  fi

  make_url=$(awk '{print "http://api.scraperapi.com/?&url=" $1}' "$file")

  run_request=$(echo "$make_url" |
    xargs -P 20 -I {} curl -G {} -s -d "binary_target=binary" \
      -d "ultra_premium=false" -d "api_key=$SCRAPERAPI_KEY" \
      -O -J -L --output-dir "$pdfpath/$subdir_name" -w "%{json}\n" |
    jq -c '{filename_effective, url, response_code}' >>~+/log.json)

  if [[ ! $run_request ]]; then
    echo "ERROR" | tee /dev/tty
    continue
  fi
done

# delete the bad PDFs and non-PDFs
compute_hash

# rename the files to their MD5 hashes
find "$pdfpath" -type f -exec md5sum {} \; | awk '{system("mv " $2 " " $1)}'

function call_on_error() {
  echo "Script exited at $(date -I)"
}

trap call_on_error EXIT

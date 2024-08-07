#! /usr/bin/bash

rawpath="raw"

mkdir $rawpath

declare today;

# get today's date to determine latest partition to download
if [[ -f $(pwd)/stages/update_dates.py ]]; then
    today=$(python3 "$(pwd)"/stages/update_dates.py);
fi

files=$(aws-cli.aws s3 ls --recursive --no-sign-request "s3://openalex/data/works/updated_date=$today" | awk '{print $4}')

for file in $files;
do
    filename="s3://openalex/$file"
    outfile="$(basename "$file" .gz).csv"
    duckdb -c "copy (select locations->'\$[0].pdf_url' as url, doi 
        from read_json('$filename', ignore_errors=true) where url is not null)
    to '$rawpath/$outfile' (HEADER false)"
done

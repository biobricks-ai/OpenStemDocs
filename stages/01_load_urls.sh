#! /usr/bin/bash

rawpath="raw"

mkdir $rawpath

files=$(aws-cli.aws s3 ls --recursive --no-sign-request "s3://openalex/data/works/updated_date=2024-06-30" | awk '{print $4}')

for file in $files;
do
    filename="s3://openalex/$file"
    outfile="$(basename "$file" .gz).csv"
    duckdb -c "copy (select locations->'\$[0].pdf_url' as url 
        from read_json('$filename', ignore_errors=true) where url is not null)
    to '$rawpath/$outfile' (HEADER false)"
done

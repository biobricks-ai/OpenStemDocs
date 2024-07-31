#! /usr/bin/bash

rawpath="raw"
pdfpath="pdfs"

mkdir $pdfpath

# get rid of triple quotes left by DuckDB
find $rawpath -type f -iname "*.csv" -exec sed -i "s/\"\"\"//g" {} \;

for file in "$rawpath"/*.csv;
do
    echo "downloading PDFs from: $file";
    wget -i "$file" --directory-prefix="$pdfpath"
done
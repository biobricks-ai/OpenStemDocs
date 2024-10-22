#!/bin/bash

run=(
    "dvc repro u00"
    "dvc repro u01"
    "dvc repro u02"
    "dvc repro u03"
)

parallel -j 4 ::: "${run[@]}"

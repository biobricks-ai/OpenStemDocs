#! /usr/bin/jq

map((if .response_code != 200 then {url, response_code} else null end) |
(if .response_code == 404 then null else .url end) | strings) | .[]

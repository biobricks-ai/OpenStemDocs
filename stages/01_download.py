import requests
from itertools import chain

url = "https://api.openalex.org/works&per_page=200&cursor=*"

search_params = {
    "filter": "is_oa:true,type:article,open_access.any_repository_has_fulltext:true",
    "per_page": 200,
    "cursor": "*",
    "mail_to": "blieberman@insilica.co",  # see: https://docs.openalex.org/how-to-use-the-api/rate-limits-and-authentication#the-polite-pool
}


def replace_cursor(url: str, new_cursor):
    return url.replace("*", new_cursor)


def get_cursor(resp):
    metadata = resp["meta"]
    if "next_cursor" in metadata:
        return metadata["next_cursor"]


def get_results(resp):
    results = resp["results"]
    urls = [
        [location["pdf_url"] for location in result["locations"]] for result in results
    ]
    return list(filter(lambda x: x is not None, chain(*urls)))


urls = []


def do_requests():
    # do initial request
    init = requests.get(url, headers={"Accept": "application/json"})
    json_resp = init.json()
    # get cursor
    cursor = json_resp["meta"]["next_cursor"]
    while cursor is not None:
        new_url = replace_cursor(url, cursor)
        next_resp = requests.get(new_url, headers={"Accept": "application/json"}).json()
        cursor = get_cursor(next_resp)
        urls.extend(get_results(next_resp))
        print(urls)


if __name__ == "__main__":
    do_requests()

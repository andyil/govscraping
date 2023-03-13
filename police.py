import argparse
import requests
from typing import NamedTuple, List, Any
import os

class FetchResult(NamedTuple):
    items: List[Any]
    skip_next: str

class ItemDataPair(NamedTuple):
    item: Any
    file: Any

def get_page(skip: str) -> FetchResult:
    res = requests.post('https://www.gov.il/api/police/menifa/api/menifa/getDocList', json={"skip": skip})
    res.raise_for_status()
    j = res.json()
    skip_next = int(skip)+len(j['Results'])
    return FetchResult(
        items=j['Results'],
        skip_next=skip_next,
    )

def collect_items():
    skip = "0"
    while True:
        res = get_page(skip)
        items = res.items
        if not items:
            return
        for item in items:
            for file in item['Data']['fileData']:
                yield ItemDataPair(item=item, file=file)
        skip = res.skip_next

def download_file(source_url, dest_filename, overwrite: bool, dryrun: bool, verbose: bool):
    res = requests.get(source_url)
    res.raise_for_status()
    # no clobber
    if overwrite or os.path.exists(dest_filename) and os.path.getsize(dest_filename):
        if verbose:
            print(dest_filename, "NOT OVERWRITING")
        return
    if not dryrun:
        with open(dest_filename, mode='wb') as f:
            f.write(res.content)
    if verbose:
        print(dest_filename)


def process_item(item, overwrite: bool, dryrun: bool, verbose: bool):
    """
    aaa {'Data': {'date_field': '01/02/2014', 'MisparPkuda': '300.01.182', 'Name': 'התייצבות משוחררים בערובה בתחנת משטרה', 'fileData': [{'FileName': 'https://www.police.gov.il/menifa/05.300.01.182_1.pdf', 'DisplayName': 'התייצבות משוחררים בערובה בתחנת משטרה', 'Extension': 'pdf'}]}}
    """
    source_url = item.file['FileName']
    dest_filename = ".".join([item.file['DisplayName'], item.file['Extension']])
    download_file(source_url, dest_filename, overwrite=overwrite, dryrun=dryrun, verbose=verbose)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("-n", "--dryrun", action="store_true", help="Do not actually download")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print downloaded filenames")
    return parser.parse_args()

def main():
    args = parse_args()
    [
        process_item(item, overwrite=args.overwrite, dryrun=args.dryrun, verbose=args.verbose)
        for item in collect_items()
    ]



if __name__=='__main__':
    main()

import argparse
import requests
from typing import NamedTuple, List, Any
import os
import time

class FetchResult(NamedTuple):
    items: List[Any]
    skip_next: str

class ItemDataPair(NamedTuple):
    item: Any
    file: Any

def get_page(skip: int = 0) -> FetchResult:
    skip = skip or 0
    print("bbbb", skip)
    res = requests.post('https://www.gov.il/he/api/DynamicCollector', json={"DynamicTemplateID":"5f3d4e58-cb49-4ab3-9248-dc85d51c072d","QueryFilters":{"skip":{"Query":skip}},"From":skip})
    res.raise_for_status()
    j = res.json()
    skip_next = skip + len(j['Results'])
    return FetchResult(
        items=j['Results'],
        skip_next=skip_next
    )

def collect_items():
    skip = None
    while True:
        res = get_page(skip)
        items = res.items
        if not items:
            return
        for item in items:
            for file in item['Data']['file']:
                yield ItemDataPair(item=item, file=file)
        skip = res.skip_next

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--overwrite", action="store_true", help="Overwrite existing files")
    parser.add_argument("-n", "--dryrun", action="store_true", help="Do not actually download")
    parser.add_argument("-v", "--verbose", action="store_true", help="Print downloaded filenames")
    return parser.parse_args()

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

    time.sleep(3000) # sleep for 3 seconds


def sanitize_filename(filename: str, itempair:ItemDataPair) -> str:
    if len(filename) < 30:
        return filename
    extension = itempair.file['Extension']
    file_serial = itempair.item['Data']['mispar']
    return ".".join([str(file_serial), extension])



def process_item(item, overwrite: bool, dryrun: bool, verbose: bool):
    """
    aaaa ItemDataPair(item={'Data': {'topic': ['5'], 'file': [...], 'title': '5.22 מעצר עד תום ההליכים בעבירות תעבורה בעילה של מסוכנות', 'search': 'מעצרים שחרור בערובה תעבורה עבירת תעבורה; תאונת דרכים; מסוכנות; מעצר עד תום ההליכים.', 'date': '2018-09-11T21:00:00Z', 'last_date': '12/09/2018', 'mispar': 522}, 'Description': None, 'UrlName': '05-022-00'}, file={'FileName': 's-a-guidelines_05.22.pdf', 'FileMime': 'application/pdf', 'FileSize': '97853', 'Extension': 'pdf', 'DisplayName': 'לעיון בהנחיה'})
    """
    source_url = f"https://www.gov.il/BlobFolder/dynamiccollectorresultitem/{item.item['UrlName']}/he/\{item.file['FileName']}"
    dest_filename = item.file['FileName']
    dest_filename = sanitize_filename(dest_filename, item)
    download_file(source_url, dest_filename, overwrite=overwrite, dryrun=dryrun, verbose=verbose)


def main():
    args = parse_args()
    [
        process_item(item, overwrite=args.overwrite, dryrun=args.dryrun, verbose=args.verbose)
        for item in collect_items()
    ]


if __name__=='__main__':
    main()

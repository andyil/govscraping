import requests
import json
import os.path
import shutil
import csv

class LandRegistration:

    def __init__(self):
        self.u = 'https://rfa.justice.gov.il/SearchPredefinedApi/Tabu/SearchPiskeiDin'
        self.workdir = os.path.join(os.path.expanduser('~'), 'land-registration')
        self.metadata = os.path.join(self.workdir, 'metadata.csv')
        self.pdfs = os.path.join(self.workdir, 'pdfs')

        self.csv_writer = None
        self.metadata_file = None

    def add_record(self, record):
        if self.csv_writer is None:
            self.metadata_file = open(self.metadata, 'w', encoding='utf-8')
            self.csv_writer = csv.DictWriter(self.metadata_file, fieldnames=record.keys(), dialect='unix')
            self.csv_writer.writeheader()

        self.csv_writer.writerow(record)

    def add_records(self, records):
        for record in records:
            self.add_record(record)

    def download_metadata(self, skip):
        body = {"skip": skip}
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        r = requests.post(self.u, json=body, headers=headers, verify=False).content
        r = r.decode('utf-8')
        r = json.loads(r)
        return r

    def get_results(self, j):
        r = j['Results']
        entries = []
        for entry in r:
            v = {}

            v.update(entry['Data'])
            del v['Document']
            if 'DocSummary' in v:
                del v['DocSummary']
            v.update(entry['Data']['Document'][0])
            v.update(entry['Data'].get('DocSummary', {}))

            entries.append(v)
        return entries

    def close(self):
        self.metadata_file.close()

    def download_all_metadata(self):
        self.prepare()

        skip = 0
        while True:
            print(f'Skip {skip}')
            j = self.download_metadata(skip)
            if len(j['Results']) == 0:
                break
            entries = self.get_results(j)
            self.add_records(entries)
            skip += len(entries)

        self.close()

    def prepare(self):
        os.path.exists(self.workdir) and shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        os.makedirs(self.pdfs)

    def download_all_pdfs(self):
        f = open(self.metadata, 'r', encoding='utf-8')
        r = csv.DictReader(f)
        for i, line in enumerate(r):
            url = line['FileName']

            filename = line['DisplayName']
            extension = line['Extension']
            destination_path = os.path.join(self.pdfs, f'{filename}-{i}.{extension}')

            if os.path.exists(destination_path):
                print(f'Skipping {destination_path}')
                continue

            resp = requests.get(url, verify=False)
            if resp.status_code == 404:
                print(f'Not found {destination_path}')
                continue
            resp.raise_for_status()
            content = resp.content


            with open(destination_path, 'wb') as w:
                w.write(content)
            print(f'Downloaded {i} to {destination_path}')

if __name__=='__main__':
    h = LandRegistration()
    h.download_all_metadata()
    h.download_all_pdfs()

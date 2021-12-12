import requests
import json
import os.path
import shutil
import csv

class StateAttorney:

    def __init__(self):
        self.u = 'https://www.gov.il/he/api/DynamicCollector'
        self.workdir = os.path.join(os.path.expanduser('~'), 'state-attorney')
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
        body = {"DynamicTemplateID": "5f3d4e58-cb49-4ab3-9248-dc85d51c072d",
                "QueryFilters": {"skip": {"Query": skip}},
                "From": skip}
        headers = {'Content-Type': 'application/json;charset=UTF-8'}
        r = requests.post(self.u, json=body, headers=headers).content
        r = r.decode('utf-8')
        r = json.loads(r)
        return r

    def get_results(self, j):
        r = j['Results']
        entries = []
        for entry in r:
            v = {}
            v['UrlName'] = entry['UrlName']
            data = entry['Data']
            fields = 'title', 'search', 'date', 'last_date', 'mispar'
            for f in fields:
                v[f] = data.get(f, '')

            file_data = data['file']
            assert len(file_data) == 1
            file_data = file_data[0]
            fields = 'FileName', 'FileMime', 'FileSize', 'Extension', 'DisplayName'
            for f in fields:
                v[f] = file_data[f]
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
            if j['TotalResults'] == 0:
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
        u = 'https://www.gov.il/BlobFolder/dynamiccollectorresultitem/{url_name}/he/{filename}'
        for i, line in enumerate(r):

            filename = line['FileName']
            url_name = line['UrlName']
            url = u.format(filename=filename, url_name=url_name)
            url = url.replace(' ', '%20')
            print(url)
            resp = requests.get(url)
            resp.raise_for_status()
            content = resp.content
            filename = url.split('/')[-1]
            destination_path = os.path.join(self.pdfs, filename)
            with open(destination_path, 'wb') as w:
                w.write(content)
            print(f'Downloaded {i} to {destination_path}')

if __name__=='__main__':
    h = StateAttorney()
    h.download_all_metadata()
    h.download_all_pdfs()

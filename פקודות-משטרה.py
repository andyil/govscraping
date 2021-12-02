import requests
import json
import os.path
import shutil
import csv

class Pkudot:

    def __init__(self):
        self.u = 'https://www.gov.il/api/police/menifa/api/menifa/getDocList'
        self.workdir = os.path.join(os.path.expanduser('~'), 'police-orders')
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
        body = {'skip': skip}
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
            data = entry['Data']
            v['date_field'] = data['date_field']
            v['mispark_pkuda'] = data['MisparPkuda']
            v['name'] = data['Name']
            file_data = data['fileData']
            assert len(file_data) == 1
            file_data = file_data[0]
            v['filename'] = file_data['FileName']
            v['DisplayName'] = file_data['DisplayName']
            v['extension'] = file_data['Extension']
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
            entries = self.get_results(j)
            self.add_records(entries)
            skip += len(entries)
            if not entries:
                break

        self.close()

    def prepare(self):
        os.path.exists(self.workdir) and shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        os.makedirs(self.pdfs)

    def download_all_pdfs(self):
        f = open(self.metadata, 'r', encoding='utf-8')
        r = csv.DictReader(f)
        for i, line in enumerate(r):
            url = line['filename']
            content = requests.get(url).content
            filename = url.split('/')[-1]
            destination_path = os.path.join(self.pdfs, filename)
            with open(destination_path, 'wb') as w:
                w.write(content)
            print(f'Downloaded {i} to {destination_path}')

if __name__=='__main__':
    h = Pkudot()
    h.download_all_metadata()
    h.download_all_pdfs()

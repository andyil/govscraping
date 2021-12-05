import requests
import json
import os.path
import shutil
import csv

class PrisonService:

    def __init__(self):
        self.u = 'https://www.gov.il/he/api/PolicyApi/Index?OfficeId=c3f24c3b-9940-45c2-82a1-c4be2087bf99&limit={limit}&keywords=&skip={skip}'
        self.workdir = os.path.join(os.path.expanduser('~'), 'prison-service-orders')
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
        print(f'Downloading skip={skip}')
        url = self.u.format(limit=skip+10, skip=skip)
        r = requests.get(url).content
        r = r.decode('utf-8')
        r = json.loads(r)
        return r

    def get_results(self, j):
        r = j['results']
        entries = []
        for entry in r:
            v = {}
            v['descriptiםn'] = entry['Description']
            v['policy_type_desc'] = entry['PolicyTypeDesc'][0]
            v['url_name'] = entry['UrlName']
            assert len(entry['FilesGroup']['FilesGroupList']) == 1
            assert len(entry['FilesGroup']['FilesGroupList'][0]['Files']) == 1
            file = entry['FilesGroup']['FilesGroupList'][0]['Files'][0]
            v['file_name'] = file['FileName']
            v['display_name'] = file['DisplayName']
            v['file_size'] = file['FileSize']
            v['file_mime'] = file['FileMime']
            v['extension'] = file['Extension']
            entries.append(v)
        return entries

    def close(self):
        self.metadata_file.close()

    def download_all_metadata(self):
        self.prepare()

        skip = 0
        while True:
            j = self.download_metadata(skip)
            entries = self.get_results(j)
            self.add_records(entries)
            skip += len(entries)
            if len(entries) < 10:
                break


        self.close()

    def prepare(self):
        print('Preparing')
        os.path.exists(self.workdir) and shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        os.makedirs(self.pdfs)
        print('Prepared')

    def download_all_pdfs(self):
        f = open(self.metadata, 'r', encoding='utf-8')
        r = csv.DictReader(f)
        for i, line in enumerate(r):
            url_name = line['url_name']
            file_name = line['file_name']
            url = f'https://www.gov.il/BlobFolder/policy/{url_name}/he/{file_name}'
            content = requests.get(url).content
            filename = url.split('/')[-1]
            destination_path = os.path.join(self.pdfs, filename)
            with open(destination_path, 'wb') as w:
                w.write(content)
            print(f'Downloaded {i} to {destination_path}')

if __name__=='__main__':
    h = PrisonService()
    h.download_all_metadata()
    h.download_all_pdfs()

import requests
import json
import os.path
import shutil
import csv
import datetime
import time
import traceback

class CollectiveAgreements:

    def __init__(self):
        self.u = 'https://apps.moital.gov.il/WorkAgreements/api/GetSelect/GetSearchResult?AgrContentPrm=&AgrNumberPrm=&EmployeePrm=&EmployerPrm=&ThirdSidePrm=&SubjectPrm=&AgrTypeIdPrm=&AgrStatusIdPrm=&BranchFatherNumberPrm=&BranchNumberPrm=null&BagNumberPrm=&DestinationPrm=&SignatureDatePrm1=01/01/1958&SignatureDatePrm2=01/01/1960&BeginDatePrm1=&BeginDatePrm2=&EndDatePrm1=&EndDatePrm2=&PresentingDatePrm1=&PresentingDatePrm2='
        self.workdir = os.path.join(os.path.expanduser('~'), 'collective-agreements')
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



    def close(self):
        self.metadata_file.close()


    def prepare(self):
        os.path.exists(self.workdir) and shutil.rmtree(self.workdir)
        os.makedirs(self.workdir)
        os.makedirs(self.pdfs)

    def download_one_document(self, index, number, extension):
        url_f = 'https://workagreements.economy.gov.il/Agreements/{number}.{extension}'

        config = {'pdf': 'application/pdf',
                  'docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document'}

        filename = f'{number}.{extension}'


        destination_path = os.path.join(self.pdfs, filename)

        last_3 = number[-3:]
        new_destination_dir = os.path.join(self.pdfs, last_3)
        new_destination_path = os.path.join(new_destination_dir, filename)

        if os.path.exists(new_destination_dir) and os.path.isfile(new_destination_dir):
            os.unlink(new_destination_dir)

        if os.path.exists(destination_path):
            try:
                shutil.move(destination_path, new_destination_path)
            except FileNotFoundError:
                os.makedirs(new_destination_dir, 0x777, True)
                shutil.move(destination_path, new_destination_path)

            print(f'Moved {destination_path} to {new_destination_path}')
            return

        if os.path.exists(new_destination_path):
            print(f'Skipping {new_destination_path}')
            return

        url = url_f.format(number=number, extension=extension)
        print(url)

        r = requests.get(url)

        headers = r.headers
        content_type = headers['Content-Type']
        if content_type != config[extension]:
            print(f'Failed {number}.{extension}, got {content_type}')
            return

        content = r.content

        if not os.path.exists(new_destination_dir):
            os.makedirs(new_destination_dir, 0x777, True)

        with open(new_destination_path, 'wb') as w:
            w.write(content)
        print(f'Downloaded {index} to {new_destination_path}')

    def download_all_documents(self):
        f = open(self.metadata, 'r', encoding='utf-8')
        r = csv.DictReader(f)

        for i, line in enumerate(r):
            number = line['AgrNumber']

            extensions = ['pdf']

            for extension in extensions:
                self.download_one_document(i, number, extension)






    def format_date(self, dt):
        d = str(dt.day).zfill(2)
        m = str(dt.month).zfill(2)
        y = str(dt.year).zfill(4)
        return f'{d}/{m}/{y}'

    def download_period(self, start, end):
        start_str = self.format_date(start)
        end_str = self.format_date(end)
        url = f'https://apps.moital.gov.il/WorkAgreements/api/GetSelect/GetSearchResult?AgrContentPrm=&AgrNumberPrm=&EmployeePrm=&EmployerPrm=&ThirdSidePrm=&SubjectPrm=&AgrTypeIdPrm=&AgrStatusIdPrm=&BranchFatherNumberPrm=&BranchNumberPrm=null&BagNumberPrm=&DestinationPrm=&SignatureDatePrm1={start_str}&SignatureDatePrm2={end_str}&BeginDatePrm1=&BeginDatePrm2=&EndDatePrm1=&EndDatePrm2=&PresentingDatePrm1=&PresentingDatePrm2='
        print(url)
        r = requests.get(url)
        c = r.content
        s = c.decode('utf-8')
        j = json.loads(s)
        if len(j) == 1 and j[0]['ErrorMessage'] == 'data-is-not-found':
            return []
        return j

    def next_month(self, dt):
        if dt.month == 12:
            next = datetime.date(dt.year + 1, 1, 1)
        else:
            next = datetime.date(dt.year, dt.month + 1, 1)

        return next

    def iterate_months(self):
        start = datetime.date(year=1948, month=1, day=1)
        today = datetime.date.today()
        while start <= today:
            next = self.next_month(start)
            next = next - datetime.timedelta(days=1)
            yield start, min(next, today)
            start = next + datetime.timedelta(days=1)

    def process_one_period(self, start, end):
        records = self.download_period(start, end)
        print(f'Records {len(records)}')
        self.add_records(records)

    def do_all_metadata(self):
        self.prepare()
        for start, end in self.iterate_months():
            print(f'Processing {start} to {end}')
            self.process_one_period(start, end)


if __name__=='__main__':
    h = CollectiveAgreements()
    h.do_all_metadata()
    h.download_all_documents()


import requests 
import gzip
import shutil
import os
import io
from bs4 import BeautifulSoup

class Data_scraper():
    def __init__(self, url : str) -> None:
        self.request = requests.get(url)
        if self.request.status_code != 200:
            raise Exception("Can't access a page")
        self.soup = BeautifulSoup(self.request.text, 'html.parser')
        self.file_dirpath = os.path.dirname(os.path.abspath(__file__))
        self.data_path = os.path.join(self.file_dirpath, 'data', 'raw')
        if not os.path.exists(self.data_path):
            os.mkdir(self.data_path)
        self.files_to_download = ('title.crew.tsv.gz', 'title.episode.tsv.gz')
    
    def get_name(self, archive):
        file = archive[:-3]
        index = file.rfind('.')
        name, ext = file[:index], file[index:]
        name = name.replace('.', '_')
        return name + ext
    
    def extract_files(self, archives):
        existing_files = os.listdir(self.data_path)
        for archive in archives:
            archive_name = archive.split('/')[-1]
            file_name = self.get_name(archive_name)
            if file_name in existing_files:
                print(f"File {file_name} already exists")
                continue
            message = f'Downloading file: {archive.split("/")[-1]}'
            print(message, end='', flush=True)
            response = requests.get(archive)
            if response.status_code != 200:
                print('\r' + " " * len(message), end='')
                print(f"\rFile {file_name} unzipped")
                continue
            content = io.BytesIO(response.content)
            try:
                print('\r' + " " * len(message), end='')
                message = f'Trying to unzip file: {file_name}'
                print(f"\r{message}", end='', flush=True)
                with gzip.open(content, 'rb') as f_in:
                    with open(os.path.join(self.data_path, file_name), 'wb') as f_out:
                        shutil.copyfileobj(f_in, f_out)
            except Exception as e:
                 print(f'\nError: {e}')
            else: 
                print('\r' + " " * len(message), end='')
                print(f"\rFile {file_name} unzipped")
    
    def get_archives(self):
        print("Archives found:")
        archives = []
        for child in self.soup.find_all('ul'):
            archive = child.a.get('href')
            if any(archive.endswith(x) for x in self.files_to_download):
                continue
            print(archive)
            archives.append(archive)
        return archives
        
    @classmethod
    def scrape(cls, url : str):
        instance = cls(url)
        archives = instance.get_archives()
        print()
        instance.extract_files(archives)
        return instance
    
if __name__ == '__main__':
    Data_scraper.scrape('https://datasets.imdbws.com/')
    print("Process Finished!")


from data_scraper import Data_scraper
from database import create_tables

if __name__ == '__main__':
    Data_scraper.scrape('https://datasets.imdbws.com/')
    create_tables()
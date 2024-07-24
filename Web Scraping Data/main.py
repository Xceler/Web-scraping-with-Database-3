import os 
import requests 
from bs4 import BeautifulSoup
from pymongo import MongoClient 
from datetime import datetime, timedelta 
from dotenv import find_dotenv, load_dotenv 

load_dotenv(find_dotenv())

password = os.environ.get('mongo_db')
connected_string = f"mongodb+srv://Bhone:{password}@web-data.bwn3y28.mongodb.net/"
client = MongoClient(connected_string)
db = client['job_data']
collection = db['jobs']

base_url = 'https://careers.accor.com/global/en/jobs?q=&options=720,258,&page='
num_pages = 13 

def scrape_page(page_num):
    url = base_url + str(page_num)
    headers = {
        'User-Agent' : 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    response = requests.get(url, headers = headers)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, 'html.parser')

    job_tiles = soup.find_all('div', class_ = 'attrax-vacancy-tile')

    job_data = [] 
    for job_tile in job_tiles:
        job_info = {}
        job_info['Title'] = job_tile.find('a', class_ = 'attrax-vacancy-tile__title').get_text(strip = True) if job_tile.find('a', class_ = 'attrax-vacancy-tile__title') else 'N/A'
        job_info['Location'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__location-freetext').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__location-freetext') else 'N/A'
        job_info['Experience_Level'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__option-experience-level').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__option-experience-level') else 'N/A'
        job_info['Job_Schedule'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-schedule').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-schedule') else 'N/A'
        job_info['Job_Type'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-type').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-type') else 'N/A'
        job_info['Brands'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__option-brands').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip= True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__option-brands') else 'N/A'
        job_info['Job_Category'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-category').find('p', class_ = 'attrax-vacancy-tile__item-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__option-job-category') else 'N/A'
        job_info['Description'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__description').find('p', class_ = 'attrax-vacancy-tile__description-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__description') else 'N/A'
        job_info['Reference'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__reference').find('p', class_  = 'attrax-vacancy-tile__reference-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__reference') else 'N/A'
        job_info['Expiry_Date'] = job_tile.find('div', class_ = 'attrax-vacancy-tile__expiry').find('p', class_ = 'attrax-vacancy-tile__expiry-value').get_text(strip = True) if job_tile.find('div', class_ = 'attrax-vacancy-tile__expiry') else 'N/A'

        if job_info['Expiry_Date'] != 'N/A':
            try:
                job_info['Expiry_Date'] = datetime.strptime(job_info['Expiry_Date'], '%Y-%m-%d')
            except ValueError:
                job_info['Expiry_Date'] = datetime.now() + timedelta(days = 7)
            
            job_info['Status'] = 'Expired' if datetime.now() > job_info['Expiry_Date'] + timedelta(days = 7) else 'Active'
            job_data.append(job_info)
        
    return job_data 


def update_mongodb():
    all_job_data = []
    for page in range(1, num_pages + 1):
        page_data= scrape_page(page)
        all_job_data.extend(page_data)


    for job in all_job_data:
        collection.update_one(
            {'reference' : job['Reference']},
            {'$set' : job},
            upsert = True
        )
    
    print(f"{len(all_job_data)} jobs scraped and updated in MongoDB")

    


if __name__ == '__main__':
    update_mongodb()
    print("Data Scraping and Update Completed")

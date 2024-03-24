import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from datetime import datetime

# Function to create or connect to the SQLite database
def create_database():
    conn = sqlite3.connect('web_scraping.db')
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS vacancies
                 (id INTEGER PRIMARY KEY,
                 position_title TEXT,
                 location TEXT,
                 added_date TEXT,
                 salary TEXT,
                 company_name TEXT,
                 due_date TEXT,
                 source TEXT,
                 timestamp TEXT)''')
    conn.commit()
    conn.close()

# Function to insert data into the database
def insert_data(data):
    conn = sqlite3.connect('web_scraping.db')
    c = conn.cursor()
    c.execute('''INSERT INTO vacancies
                 (position_title, location, added_date, salary, company_name, due_date, source, timestamp)
                 VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', data)
    conn.commit()
    conn.close()

# Function to scrape the website and retrieve data
def scrape_website():
    base_url = 'https://www.visidarbi.lv/darba-sludinajumi?page='
    page_number = 1
    total_rows = 0
    start_time = time.time()

    while True:
        url = base_url + str(page_number)
        response = requests.get(url)
        if response.status_code == 200:
            soup = BeautifulSoup(response.text, 'html.parser')
            boxes = soup.find_all(class_=lambda x: x and x.startswith("box"))
            if not boxes:
                break
            for box in boxes:
                position_title = box.find('h3').text.strip()
                location = box.find(class_='location')
                location = location.text.strip() if location else None
                added_date = box.find(class_='posted').text.strip()
                salary = box.find(class_='salary')
                salary = salary.text.strip() if salary else None
                company_name = box.find(class_='company-name')
                company_name = company_name.text.strip() if company_name else None
                due_date = box.find(class_='date')
                due_date = due_date.text.strip() if due_date else None
                source = url
                timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                data = (position_title, location, added_date, salary, company_name, due_date, source, timestamp)
                insert_data(data)
                total_rows += 1
            page_number += 1
        else:
            print("Error fetching page:", url)
            break

    end_time = time.time()
    duration = end_time - start_time
    print("Scraping completed in {:.2f} seconds".format(duration))
    print("Total rows retrieved:", total_rows)

if __name__ == "__main__":
    try:
        create_database()
        scrape_website()
    except Exception as e:
        print("An error occurred:", str(e))
    finally:
        print("Script completed at:", datetime.now())

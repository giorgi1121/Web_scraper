import requests
from bs4 import BeautifulSoup
import sqlite3
import time
from datetime import datetime


# Function to create the vacancies table
def create_table():
    # Establish a connection to the SQLite database
    conn = sqlite3.connect('vacancies.db')
    c = conn.cursor()

    # Create a table if it doesn't exist
    c.execute('''CREATE TABLE IF NOT EXISTS vacancies
                 (id INTEGER PRIMARY KEY, 
                 title TEXT, 
                 location TEXT, 
                 added TEXT, 
                 salary TEXT, 
                 company TEXT, 
                 duedate TEXT,
                 source TEXT,
                 timestamp TEXT)''')

    # Commit changes and close the connection
    conn.commit()
    conn.close()


# Function to scrape the website and extract data
def scrape_and_store(url):
    # Establish a connection to the SQLite database
    conn = sqlite3.connect('vacancies.db')
    c = conn.cursor()

    # Retrieve the webpage content
    response = requests.get(url)
    soup = BeautifulSoup(response.content, 'html.parser')

    # Find all job items
    job_items = soup.find_all('div', class_='item premium big-item')

    # Iterate over job items
    for job in job_items:
        title = job.find('h3').text.strip()
        location = job.find('li', class_='location').text.strip()
        added = job.find('li', class_='added').text.strip()
        salary = job.find('li', class_='salary').text.strip()
        company = job.find('li', class_='company').text.strip()
        duedate = job.find('li', class_='duedate').text.strip()
        source = url
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        # Insert the data into the database
        c.execute('''INSERT INTO vacancies (title, location, added, salary, company, duedate, source, timestamp)
                     VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                  (title, location, added, salary, company, duedate, source, timestamp))

    # Commit changes and close the connection
    conn.commit()
    conn.close()


# Function to run the scraping process
def main():
    create_table()

    start_time = time.time()

    # Loop through the pages
    for page_num in range(1, 11):  # Assuming there are 10 pages
        url = f'https://www.visidarbi.lv/darba-sludinajumi?page={page_num}#results'
        scrape_and_store(url)

    end_time = time.time()
    duration = end_time - start_time

    # Log the details
    with open('log.txt', 'a') as logfile:
        logfile.write(f'Start Time: {datetime.now()}\n')
        logfile.write(f'End Time: {datetime.now()}\n')
        logfile.write(f'Duration: {duration} seconds\n')
        logfile.write(f'Total Data Rows Retrieved: {page_num * 20}\n')


# Execute the main function
if __name__ == "__main__":
    main()

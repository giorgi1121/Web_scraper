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


# Define a user agent
USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3'


# Function to scrape the website and extract data
def scrape_and_store(url):
    # Retry up to 3 times
    for _ in range(3):
        try:
            response = requests.get(url, headers={'User-Agent': USER_AGENT})
            if response.status_code == 200:
                soup = BeautifulSoup(response.content, 'html.parser')
                # Your scraping logic here
                conn = sqlite3.connect('vacancies.db')
                c = conn.cursor()

                # Find all job items
                job_items = soup.find_all('div', class_=[
                    'item premium premium-plan big-item',
                    'item premium premium-plan deadline big-item',
                    'item premium big-item',
                    'item big-item',
                    'item',
                    'list-banner'
                ])

                # Iterate over job items
                for job in job_items:
                    try:
                        title_element = job.find('h3')
                        title = title_element.text.strip() if title_element else None
                        location_element = job.find('li', class_='location')
                        location = location_element.text.strip() if location_element else None
                        added_element = job.find('li', class_='added')
                        added = added_element.text.strip() if added_element else None
                        salary_element = job.find('li', class_='salary')
                        salary = salary_element.text.strip() if salary_element else None
                        company_element = job.find('li', class_='company')
                        company = company_element.text.strip() if company_element else None
                        duedate_element = job.find('li', class_='duedate')
                        duedate = duedate_element.text.strip() if duedate_element else None
                        source = url
                        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                        # Insert the data into the database
                        c.execute('''INSERT INTO vacancies (title, location, added, salary, company, duedate, source, timestamp)
                                                         VALUES (?, ?, ?, ?, ?, ?, ?, ?)''',
                                  (title, location, added, salary, company, duedate, source, timestamp))
                    except Exception as e:
                        print(f"Failed to parse job item: {e}")

                # Commit changes and close the connection
                conn.commit()
                conn.close()
                return soup
        except Exception as e:
            print(f"Error occurred: {e}")
            time.sleep(1)  # Add a short delay before retrying
    return None


# Function to run the scraping process
def main():
    create_table()

    start_time = time.time()

    # Loop through the pages
    page_num = 1
    while True:
        url = f'https://www.visidarbi.lv/'
        # url = f'https://www.visidarbi.lv/darba-sludinajumi?page={page_num}#results'
        response = requests.get(url)
        if response.status_code == 200:
            scrape_and_store(url)  # Pass the dynamically generated URL
            page_num += 1
        else:
            print(f"No more pages available. Last page scraped: {page_num - 1}")
            break

    end_time = time.time()
    duration = end_time - start_time

    # Log the details
    with open('log.txt', 'a') as logfile:
        logfile.write(f'Start Time: {datetime.fromtimestamp(start_time)}\n')
        logfile.write(f'End Time: {datetime.fromtimestamp(end_time)}\n')
        logfile.write(f'Duration: {duration} seconds\n')
        # Assuming each page has 20 job items
        total_rows = (page_num - 1) * 20
        logfile.write(f'Total Data Rows Retrieved: {total_rows}\n')
        logfile.write("-" * 20)


# Execute the main function
if __name__ == "__main__":
    main()

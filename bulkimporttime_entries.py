from pyconnectwise import ConnectWiseManageAPIClient
from pyconnectwise.config import Config
from datetime import datetime
import argparse
import json
import mysql.connector


## This script will connect to the connectwise api and pull all data, one page at a time and dump it into a mysql database

# Initialize the ConnectWise client
manage_api_client = ConnectWiseManageAPIClient(
    "pundit",
    "na.myconnectwise.net",
    "d0eb15de-c43c-4d2c-af2a-cdf4b1ac7f8b",
    "9BWuVfvLn1hwalDY",
    "labeqwaNcrdA7JkR"
)

# Connect to MySQL
conn = mysql.connector.connect(
    host="10.255.249.50",
    user="pundit",
    password="pun!Zlrn6006",
    database="connectwise"
)
cursor = conn.cursor()

# Create the time_entries table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS time_entries (
        id INT PRIMARY KEY,
        member_identifier VARCHAR(255),
        date DATETIME,
        hours FLOAT,
        notes TEXT
    )
''')

# Fetch paginated time entries data
paginated_time_entries = manage_api_client.time.entries.paginated(1, 1000)

page_number = 1
while True:
    page_data = paginated_time_entries.data
    print(f"Page {page_number} data: {len(page_data)}")
    
    for entry in page_data:
        cursor.execute('''SELECT COUNT(*) FROM time_entries WHERE id = %s''', (entry.id,))
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO time_entries (id, member_identifier, date, hours, notes)
                VALUES (%s, %s, %s, %s, %s)
            ''', (entry.id, entry.member.identifier, entry.time_start, entry.actual_hours, entry.notes))
    
    conn.commit()
    
    if not paginated_time_entries.has_next_page:
        break
    
    paginated_time_entries.get_next_page()
    page_number += 1

conn.close()

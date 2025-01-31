from pyconnectwise import ConnectWiseManageAPIClient
from pyconnectwise.config import Config
from datetime import datetime
import argparse
import json
from creds import connectwise_credentials, mysql_credentials
import mysql.connector


## This script will connect to the connectwise api and pull all data, one page at a time and dump it into a mysql database

# Initialize the ConnectWise client
manage_api_client = ConnectWiseManageAPIClient(
    connectwise_credentials["company_id"],
    connectwise_credentials["site"],
    connectwise_credentials["public_key"],
    connectwise_credentials["private_key"],
    connectwise_credentials["client_id"]
)

# Connect to MySQL
conn = mysql.connector.connect(
    host=mysql_credentials["host"],
    user=mysql_credentials["user"],
    password=mysql_credentials["password"],
    database=mysql_credentials["database"]
)
cursor = conn.cursor()

# Create the time_sheets table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS time_sheets (
        id INT PRIMARY KEY,
        member_identifier VARCHAR(255),
        member_id INT,
        datestart DATETIME,
        dateend DATETIME,
        status VARCHAR(255),
        hours FLOAT
    )
''')

# Fetch paginated time sheets data
paginated_time_sheets = manage_api_client.time.sheets.paginated(1, 1000)

page_number = 1
while True:
    page_data = paginated_time_sheets.data
    print(f"Page {page_number} data: {len(page_data)}")
    
    for sheet in page_data:
        cursor.execute('''SELECT COUNT(*) FROM time_sheets WHERE id = %s''', (sheet.id,))
        if cursor.fetchone()[0] == 0:
            cursor.execute('''
                INSERT INTO time_sheets (id, member_identifier, member_id, datestart, dateend, status, hours)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
            ''', (sheet.id, sheet.member.identifier, sheet.member.id, sheet.date_start, sheet.date_end, sheet.status, sheet.hours))
        else:
            cursor.execute('''
                UPDATE time_sheets
                SET member_identifier = %s, member_id = %s, datestart = %s, dateend = %s, status = %s, hours = %s
                WHERE id = %s
            ''', (sheet.member.identifier, sheet.member.id, sheet.date_start, sheet.date_end, sheet.status, sheet.hours, sheet.id))
        conn.commit()
        #print(f"ID: {sheet.id}\n, Member Identifier: {sheet.member.identifier}\n, Member ID: {sheet.member.id}\n, Start Date: {sheet.date_start}\n, End Date: {sheet.date_end}\n, Status: {sheet.status}\n, Hours: {sheet.hours}\n")
        #pause = input("Press enter to continue")
    
    if not paginated_time_sheets.has_next_page:
        break
    
    paginated_time_sheets.get_next_page()
    page_number += 1

conn.close()

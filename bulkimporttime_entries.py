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

# Create the time_entries table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS time_entries (
        id INT PRIMARY KEY,
        company_identifier VARCHAR(255),
        company_id INT,
        member_identifier VARCHAR(255),
        time_start DATETIME,
        time_end DATETIME,
        hours FLOAT,
        billable_option VARCHAR(255),
        notes TEXT,
        internal_notes TEXT,
        date_entered DATETIME,
        timesheet_id INT,
        timesheet_name VARCHAR(255),
        status VARCHAR(255)
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
                INSERT INTO time_entries (id, company_identifier, company_id, member_identifier, time_start, time_end, hours, billable_option, notes, internal_notes, date_entered, timesheet_id, timesheet_name, status)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ''', (entry.id, entry.company.identifier, entry.company.id, entry.member.identifier, entry.time_start, entry.time_end, entry.actual_hours, entry.billable_option, entry.notes, entry.internal_notes, entry.date_entered, entry.time_sheet.id, entry.time_sheet.name, entry.status))
        else:
            cursor.execute('''
                UPDATE time_entries
                SET company_identifier = %s, company_id = %s, member_identifier = %s, time_start = %s, time_end = %s, hours = %s, billable_option = %s, notes = %s, internal_notes = %s, date_entered = %s, timesheet_id = %s, timesheet_name = %s, status = %s
                WHERE id = %s
            ''', (entry.company.identifier, entry.company.id, entry.member.identifier, entry.time_start, entry.time_end, entry.actual_hours, entry.billable_option, entry.notes, entry.internal_notes, entry.date_entered, entry.time_sheet.id, entry.time_sheet.name, entry.status, entry.id))
    
        conn.commit()
        #print(f"ID: {entry.id}\n, Company Identifier: {entry.company.identifier}\n, Company ID: {entry.company.id}\n, Member Identifier: {entry.member.identifier}\n, Time Start: {entry.time_start}\n, Time End: {entry.time_end}\n, Hours: {entry.actual_hours}\n, Billable Option: {entry.billable_option}\n, Notes: {entry.notes}\n, Internal Notes: {entry.internal_notes}\n, Date Entered: {entry.date_entered}\n, Timesheet ID: {entry.time_sheet.id}\n, Timesheet Name: {entry.time_sheet.name}\n, Status: {entry.status}\n")
        #pause = input("Press enter to continue")
    
    if not paginated_time_entries.has_next_page:
        break
    
    paginated_time_entries.get_next_page()
    page_number += 1

conn.close()

from pyconnectwise import ConnectWiseManageAPIClient
from pyconnectwise.config import Config
from datetime import datetime
import argparse
import json
from creds import connectwise_credentials, mysql_credentials
import mysql.connector
import sys

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

# Create the timeentries table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS timeentries (
        id INT PRIMARY KEY,
        company_id INT,
        company_identifier VARCHAR(255),
        company_name VARCHAR(255),
        companyType VARCHAR(255),
        chargeToId INT,
        chargeToType VARCHAR(255),
        member_id INT,
        member_identifier VARCHAR(255),
        member_name VARCHAR(255),
        timeStart DATETIME,
        timeEnd DATETIME,
        hoursDeduct FLOAT,
        actualHours FLOAT,
        billableOption VARCHAR(255),
        notes TEXT,
        internalNotes TEXT,
        hoursBilled FLOAT,
        invoiceHours FLOAT,
        enteredBy VARCHAR(255),
        dateEntered DATETIME,
        hourlyRate FLOAT,
        invoiceReady BOOLEAN,
        timeSheet_id INT,
        timeSheet_name VARCHAR(255),
        status VARCHAR(255),
        ticket_id INT,
        ticket_summary VARCHAR(255),
        project_id INT,
        project_name VARCHAR(255),
        phase_id INT,
        phase_name VARCHAR(255),
        ticketBoard VARCHAR(255),
        ticketStatus VARCHAR(255),
        ticketType VARCHAR(255),
        ticketSubType VARCHAR(255),
        invoiceFlag BOOLEAN
    )
''')

def update_time_entries():
    # Fetch the last entry's ID from the database
    cursor.execute('''SELECT MAX(id) FROM timeentries''')
    last_entry_id = cursor.fetchone()[0] or 0
    print(f"Last entry ID: {last_entry_id}")
    print(f"Fetching time entries...after {last_entry_id}")    

    # Fetch paginated time entries data starting from the beginning
    paginated_time_entries = manage_api_client.time.entries.paginated(1, 1000)

    page_number = 1
    while True:
        page_data = paginated_time_entries.data
        print(f"Page {page_number} data: {len(page_data)}")
        
        for entry in page_data:
            if entry.id > last_entry_id:
                try:
                    cursor.execute('''SELECT COUNT(*) FROM timeentries WHERE id = %s''', (entry.id,))
                    if cursor.fetchone()[0] == 0:
                        cursor.execute('''
                            INSERT INTO timeentries (
                                id, company_id, company_identifier, company_name, companyType, chargeToId, chargeToType, member_id, member_identifier, member_name,
                                timeStart, timeEnd, hoursDeduct, actualHours, billableOption, notes, internalNotes, hoursBilled, invoiceHours,
                                enteredBy, dateEntered, hourlyRate, invoiceReady, timeSheet_id, timeSheet_name, status, ticket_id, ticket_summary, project_id, project_name,
                                phase_id, phase_name, ticketBoard, ticketStatus, ticketType, ticketSubType, invoiceFlag
                            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ''', (
                            entry.id if hasattr(entry, 'id') else None,
                            entry.company.id if hasattr(entry.company, 'id') else None,
                            entry.company.identifier if hasattr(entry.company, 'identifier') else None,
                            entry.company.name if hasattr(entry.company, 'name') else None,
                            entry.companyType if hasattr(entry, 'companyType') else None,
                            entry.chargeToId if hasattr(entry, 'chargeToId') else None,
                            entry.chargeToType if hasattr(entry, 'chargeToType') else None,
                            entry.member.id if hasattr(entry.member, 'id') else None,
                            entry.member.identifier if hasattr(entry.member, 'identifier') else None,
                            entry.member.name if hasattr(entry.member, 'name') else None,
                            entry.timeStart if hasattr(entry, 'timeStart') else None,
                            entry.timeEnd if hasattr(entry, 'timeEnd') else None,
                            entry.hoursDeduct if hasattr(entry, 'hoursDeduct') else None,
                            entry.actualHours if hasattr(entry, 'actualHours') else None,
                            entry.billableOption if hasattr(entry, 'billableOption') else None,
                            entry.notes if hasattr(entry, 'notes') else None,
                            entry.internalNotes if hasattr(entry, 'internalNotes') else None,
                            entry.hoursBilled if hasattr(entry, 'hoursBilled') else None,
                            entry.invoiceHours if hasattr(entry, 'invoiceHours') else None,
                            entry.enteredBy if hasattr(entry, 'enteredBy') else None,
                            entry.dateEntered if hasattr(entry, 'dateEntered') else None,
                            entry.hourlyRate if hasattr(entry, 'hourlyRate') else None,
                            entry.invoiceReady if hasattr(entry, 'invoiceReady') else None,
                            entry.timeSheet.id if hasattr(entry, 'timeSheet') and hasattr(entry.timeSheet, 'id') else None,
                            entry.timeSheet.name if hasattr(entry, 'timeSheet') and hasattr(entry.timeSheet, 'name') else None,
                            entry.status if hasattr(entry, 'status') else None,
                            entry.ticket.id if hasattr(entry, 'ticket') and hasattr(entry.ticket, 'id') else None,
                            entry.ticket.summary if hasattr(entry, 'ticket') and hasattr(entry.ticket, 'summary') else None,
                            entry.project.id if hasattr(entry, 'project') and hasattr(entry.project, 'id') else None,
                            entry.project.name if hasattr(entry, 'project') and hasattr(entry.project, 'name') else None,
                            entry.phase.id if hasattr(entry, 'phase') and hasattr(entry.phase, 'id') else None,
                            entry.phase.name if hasattr(entry, 'phase') and hasattr(entry.phase, 'name') else None,
                            entry.ticketBoard if hasattr(entry, 'ticketBoard') else None,
                            entry.ticketStatus if hasattr(entry, 'ticketStatus') else None,
                            entry.ticketType if hasattr(entry, 'ticketType') else None,
                            entry.ticketSubType if hasattr(entry, 'ticketSubType') else None,
                            entry.invoiceFlag if hasattr(entry, 'invoiceFlag') else None
                        ))
                        conn.commit()
                except mysql.connector.Error as err:
                    print(f"Error: {err}")
                    continue
        
        if not paginated_time_entries.has_next_page:
            break
        
        paginated_time_entries.get_next_page()
        page_number += 1

def import_all_time_entries():
    # Fetch paginated time entries data starting from the beginning
    paginated_time_entries = manage_api_client.time.entries.paginated(1, 1000)

    page_number = 1
    while True:
        page_data = paginated_time_entries.data
        print(f"Page {page_number} data: {len(page_data)}")
        
        for entry in page_data:
            try:
                cursor.execute('''SELECT COUNT(*) FROM timeentries WHERE id = %s''', (entry.id,))
                if cursor.fetchone()[0] == 0:
                    cursor.execute('''
                        INSERT INTO timeentries (
                            id, company_id, company_identifier, company_name, companyType, chargeToId, chargeToType, member_id, member_identifier, member_name,
                            timeStart, timeEnd, hoursDeduct, actualHours, billableOption, notes, internalNotes, hoursBilled, invoiceHours,
                            enteredBy, dateEntered, hourlyRate, invoiceReady, timeSheet_id, timeSheet_name, status, ticket_id, ticket_summary, project_id, project_name,
                            phase_id, phase_name, ticketBoard, ticketStatus, ticketType, ticketSubType, invoiceFlag
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ''', (
                        entry.id if hasattr(entry, 'id') else None,
                        entry.company.id if hasattr(entry.company, 'id') else None,
                        entry.company.identifier if hasattr(entry.company, 'identifier') else None,
                        entry.company.name if hasattr(entry.company, 'name') else None,
                        entry.companyType if hasattr(entry, 'companyType') else None,
                        entry.chargeToId if hasattr(entry, 'chargeToId') else None,
                        entry.chargeToType if hasattr(entry, 'chargeToType') else None,
                        entry.member.id if hasattr(entry.member, 'id') else None,
                        entry.member.identifier if hasattr(entry.member, 'identifier') else None,
                        entry.member.name if hasattr(entry.member, 'name') else None,
                        entry.timeStart if hasattr(entry, 'timeStart') else None,
                        entry.timeEnd if hasattr(entry, 'timeEnd') else None,
                        entry.hoursDeduct if hasattr(entry, 'hoursDeduct') else None,
                        entry.actualHours if hasattr(entry, 'actualHours') else None,
                        entry.billableOption if hasattr(entry, 'billableOption') else None,
                        entry.notes if hasattr(entry, 'notes') else None,
                        entry.internalNotes if hasattr(entry, 'internalNotes') else None,
                        entry.hoursBilled if hasattr(entry, 'hoursBilled') else None,
                        entry.invoiceHours if hasattr(entry, 'invoiceHours') else None,
                        entry.enteredBy if hasattr(entry, 'enteredBy') else None,
                        entry.dateEntered if hasattr(entry, 'dateEntered') else None,
                        entry.hourlyRate if hasattr(entry, 'hourlyRate') else None,
                        entry.invoiceReady if hasattr(entry, 'invoiceReady') else None,
                        entry.timeSheet.id if hasattr(entry, 'timeSheet') and hasattr(entry.timeSheet, 'id') else None,
                        entry.timeSheet.name if hasattr(entry, 'timeSheet') and hasattr(entry.timeSheet, 'name') else None,
                        entry.status if hasattr(entry, 'status') else None,
                        entry.ticket.id if hasattr(entry, 'ticket') and hasattr(entry.ticket, 'id') else None,
                        entry.ticket.summary if hasattr(entry, 'ticket') and hasattr(entry.ticket, 'summary') else None,
                        entry.project.id if hasattr(entry, 'project') and hasattr(entry.project, 'id') else None,
                        entry.project.name if hasattr(entry, 'project') and hasattr(entry.project, 'name') else None,
                        entry.phase.id if hasattr(entry, 'phase') and hasattr(entry.phase, 'id') else None,
                        entry.phase.name if hasattr(entry, 'phase') and hasattr(entry.phase, 'name') else None,
                        entry.ticketBoard if hasattr(entry, 'ticketBoard') else None,
                        entry.ticketStatus if hasattr(entry, 'ticketStatus') else None,
                        entry.ticketType if hasattr(entry, 'ticketType') else None,
                        entry.ticketSubType if hasattr(entry, 'ticketSubType') else None,
                        entry.invoiceFlag if hasattr(entry, 'invoiceFlag') else None
                    ))
                    conn.commit()
            except mysql.connector.Error as err:
                print(f"Error: {err}")
                continue
        
        if not paginated_time_entries.has_next_page:
            break
        
        paginated_time_entries.get_next_page()
        page_number += 1

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Import time entries from ConnectWise to MySQL.')
    parser.add_argument('--update', action='store_true', help='Update time entries from the last entry forward')
    parser.add_argument('--all', action='store_true', help='Import all time entries from the beginning')
    args = parser.parse_args()

    if args.update:
        update_time_entries()
    elif args.all:
        import_all_time_entries()
    else:
        print("Please specify --update or --all")

    conn.close()

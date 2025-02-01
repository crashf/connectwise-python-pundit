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

# Create the service_tickets table if it doesn't exist
cursor.execute('''
    CREATE TABLE IF NOT EXISTS service_tickets (
        id INT PRIMARY KEY,
        summary VARCHAR(255),
        status VARCHAR(255),
        priority VARCHAR(255),
        service_board VARCHAR(255),
        company_id INT,
        company_name VARCHAR(255),
        contact_id INT,
        contact_name VARCHAR(255),
        assigned_resource_id INT,
        assigned_resource_name VARCHAR(255),
        date_entered DATETIME,
        date_resolved DATETIME,
        date_closed DATETIME,
        date_due DATETIME,
        date_escalated DATETIME,
        date_scheduled_start DATETIME,
        date_scheduled_end DATETIME,
        date_rescheduled DATETIME,
        date_last_updated DATETIME,
        closed_by_resource_id INT,
        closed_by_resource_name VARCHAR(255),
        resolution TEXT,
        closed_flag BOOLEAN
    )
''')

# Fetch paginated service tickets data
paginated_service_tickets = manage_api_client.service.tickets.paginated(1, 1000)

page_number = 1
while True:
    page_data = paginated_service_tickets.data
    print(f"Page {page_number} data: {len(page_data)}")
    
    for ticket in page_data:
        cursor.execute('''SELECT COUNT(*) FROM service_tickets WHERE id = %s''', (ticket.id,))
        if cursor.fetchone()[0] == 0:
            if ticket.closed_flag:
                cursor.execute('''
                    INSERT INTO service_tickets (id, summary, status, priority, service_board, company_id, company_name, contact_id, contact_name, assigned_resource_id, assigned_resource_name, date_entered, date_resolved, date_closed, date_due, date_escalated, date_scheduled_start, date_scheduled_end, date_rescheduled, date_last_updated, closed_by_resource_id, closed_by_resource_name, resolution, closed_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (ticket.id, ticket.summary, ticket.status.name, ticket.priority.name, ticket.board.name, ticket.company.id, ticket.company.name, ticket.contact.id, ticket.contact.name, ticket.assigned_resource.id, ticket.assigned_resource.name, ticket.date_entered, ticket.date_resolved, ticket.date_closed, ticket.date_due, ticket.date_escalated, ticket.date_scheduled_start, ticket.date_scheduled_end, ticket.date_rescheduled, ticket.date_last_updated, ticket.closed_by_resource.id, ticket.closed_by_resource.name, ticket.resolution, ticket.closed_flag))
                print(f"ID: {ticket.id}\nSummary: {ticket.summary}\nStatus: {ticket.status.name}\nPriority: {ticket.priority.name}\nService Board: {ticket.board.name}\nCompany ID: {ticket.company.id}\nCompany Name: {ticket.company.name}\nContact ID: {ticket.contact.id}\nContact Name: {ticket.contact.name}\nAssigned Resource ID: {ticket.assigned_resource.id}\nAssigned Resource Name: {ticket.assigned_resource.name}\nDate Entered: {ticket.date_entered}\nDate Resolved: {ticket.date_resolved}\nDate Closed: {ticket.date_closed}\nDate Due: {ticket.date_due}\nDate Escalated: {ticket.date_escalated}\nDate Scheduled Start: {ticket.date_scheduled_start}\nDate Scheduled End: {ticket.date_scheduled_end}\nDate Rescheduled: {ticket.date_rescheduled}\nDate Last Updated: {ticket.date_last_updated}\nClosed By Resource ID: {ticket.closed_by_resource.id}\nClosed By Resource Name: {ticket.closed_by_resource.name}\nResolution: {ticket.resolution}\nClosed Flag: {ticket.closed_flag}\n")
            else:
                cursor.execute('''
                    INSERT INTO service_tickets (id, summary, status, priority, service_board, company_id, company_name, contact_id, contact_name, assigned_resource_id, assigned_resource_name, date_entered, date_resolved, date_closed, date_due, date_escalated, date_scheduled_start, date_scheduled_end, date_rescheduled, date_last_updated, closed_by_resource_id, closed_by_resource_name, resolution, closed_flag)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (ticket.id, ticket.summary, ticket.status.name, ticket.priority.name, ticket.board.name, ticket.company.id, ticket.company.name, ticket.contact.id, ticket.contact.name, ticket.assigned_resource.id, ticket.assigned_resource.name, ticket.date_entered, ticket.date_resolved, ticket.date_closed, ticket.date_due, ticket.date_escalated, ticket.date_scheduled_start, ticket.date_scheduled_end, ticket.date_rescheduled, ticket.date_last_updated, ticket.closed_by_resource.id, ticket.closed_by_resource.name, ticket.resolution, ticket.closed_flag))
                print(f"ID: {ticket.id}\nSummary: {ticket.summary}\nStatus: {ticket.status.name}\nPriority: {ticket.priority.name}\nService Board: {ticket.board.name}\nCompany ID: {ticket.company.id}\nCompany Name: {ticket.company.name}\nContact ID: {ticket.contact.id}\nContact Name: {ticket.contact.name}\nAssigned Resource ID: {ticket.assigned_resource.id}\nAssigned Resource Name: {ticket.assigned_resource.name}\nDate Entered: {ticket.date_entered}\nDate Resolved: {ticket.date_resolved}\nDate Closed: {ticket.date_closed}\nDate Due: {ticket.date_due}\nDate Escalated: {ticket.date_escalated}\nDate Scheduled Start: {ticket.date_scheduled_start}\nDate Scheduled End: {ticket.date_scheduled_end}\nDate Rescheduled: {ticket.date_rescheduled}\nDate Last Updated: {ticket.date_last_updated}\nClosed By Resource ID: {ticket.closed_by_resource.id}\nClosed By Resource Name: {ticket.closed_by_resource.name}\nResolution: {ticket.resolution}\nClosed Flag: {ticket.closed_flag}\n")
        else:
            cursor.execute('''
                UPDATE service_tickets
                SET summary = %s, status = %s, priority = %s, service_board = %s, company_id = %s, company_name = %s, contact_id = %s, contact_name = %s, assigned_resource_id = %s, assigned_resource_name = %s, date_entered = %s, date_resolved = %s, date_closed = %s, date_due = %s, date_escalated = %s, date_scheduled_start = %s, date_scheduled_end = %s, date_rescheduled = %s, date_last_updated = %s, closed_by_resource_id = %s, closed_by_resource_name = %s, resolution = %s, closed_flag = %s
                WHERE id = %s
            ''', (ticket.summary, ticket.status.name, ticket.priority.name, ticket.board.name, ticket.company.id, ticket.company.name, ticket.contact.id, ticket.contact.name, ticket.assigned_resource.id, ticket.assigned_resource.name, ticket.date_entered, ticket.date_resolved, ticket.date_closed, ticket.date_due, ticket.date_escalated, ticket.date_scheduled_start, ticket.date_scheduled_end, ticket.date_rescheduled, ticket.date_last_updated, ticket.closed_by_resource.id, ticket.closed_by_resource.name, ticket.resolution, ticket.closed_flag, ticket.id))
            print(f"ID: {ticket.id}\nSummary: {ticket.summary}\nStatus: {ticket.status.name}\nPriority: {ticket.priority.name}\nService Board: {ticket.board.name}\nCompany ID: {ticket.company.id}\nCompany Name: {ticket.company.name}\nContact ID: {ticket.contact.id}\nContact Name: {ticket.contact.name}\nAssigned Resource ID: {ticket.assigned_resource.id}\nAssigned Resource Name: {ticket.assigned_resource.name}\nDate Entered: {ticket.date_entered}\nDate Resolved: {ticket.date_resolved}\nDate Closed: {ticket.date_closed}\nDate Due: {ticket.date_due}\nDate Escalated: {ticket.date_escalated}\nDate Scheduled Start: {ticket.date_scheduled_start}\nDate Scheduled End: {ticket.date_scheduled_end}\nDate Rescheduled: {ticket.date_rescheduled}\nDate Last Updated: {ticket.date_last_updated}\nClosed By Resource ID: {ticket.closed_by_resource.id}\nClosed By Resource Name: {ticket.closed_by_resource.name}\nResolution: {ticket.resolution}\nClosed Flag: {ticket.closed_flag}\n")
    
        #onn.commit()
        pause = input("Press enter to continue")
    
    if not paginated_service_tickets.has_next_page:
        break
    
    paginated_service_tickets.get_next_page()
    page_number += 1

conn.close()

from pyconnectwise import ConnectWiseManageAPIClient
from pyconnectwise.config import Config
from datetime import datetime
import mysql.connector
from creds import connectwise_credentials, mysql_credentials


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
        board_id INT,
        board_name VARCHAR(255),
        status_id INT,
        status_name VARCHAR(255),
        status_sort INT,
        company_id INT,
        company_identifier VARCHAR(255),
        company_name VARCHAR(255),
        severity VARCHAR(255),
        impact VARCHAR(255),
        customer_updated_flag BOOLEAN,
        initial_description TEXT,
        initial_internal_analysis TEXT,
        initial_resolution TEXT,
        initial_description_from VARCHAR(255),
        process_notifications BOOLEAN,
        skip_callback BOOLEAN,
        closed_date DATETIME,
        closed_by VARCHAR(255),
        closed_flag BOOLEAN,
        actual_hours FLOAT,
        approved BOOLEAN,
        date_resolved DATETIME,
        date_resplan DATETIME,
        date_responded DATETIME,
        resolve_minutes INT,
        res_plan_minutes INT,
        respond_minutes INT,
        is_in_sla BOOLEAN,
        knowledge_base_link_id INT,
        resources TEXT,
        parent_ticket_id INT,
        has_child_ticket BOOLEAN,
        has_merged_child_ticket_flag BOOLEAN,
        knowledge_base_link_type VARCHAR(255),
        bill_time VARCHAR(255),
        bill_expenses VARCHAR(255),
        bill_products VARCHAR(255),
        lag_days INT,
        lag_nonworking_days_flag BOOLEAN,
        estimated_start_date DATETIME,
        duration FLOAT,
        sla_status VARCHAR(255),
        request_for_change_flag BOOLEAN,
        escalation_start_date_utc DATETIME,
        escalation_level INT,
        minutes_before_waiting INT,
        responded_skipped_minutes INT,
        resplan_skipped_minutes INT,
        responded_hours FLOAT,
        responded_by VARCHAR(255),
        resplan_hours FLOAT,
        resplan_by VARCHAR(255),
        resolution_hours FLOAT,
        resolved_by VARCHAR(255),
        minutes_waiting INT
    )
''')

# Function to convert empty fields to NULL
def clean_value(value):
    if value is None or value == "" or value == "null":
        return None
    return value

# Function to convert date fields
def convert_datetime(dt_str):
    if dt_str:
        try:
            return datetime.strptime(dt_str, '%Y-%m-%dT%H:%M:%SZ')
        except ValueError:
            return None
    return None

# Fetch paginated service tickets data
paginated_service_tickets = manage_api_client.service.tickets.paginated(1, 1000)

page_number = 1
while True:
    page_data = paginated_service_tickets.data
    print(f"Page {page_number} data: {len(page_data)}")
    
    for ticket in page_data:
        try:
            cursor.execute("SELECT COUNT(*) FROM service_tickets WHERE id = %s", (ticket.id,))
            if cursor.fetchone()[0] == 0:
                cursor.execute('''
                    INSERT INTO service_tickets (
                        id, summary, board_id, board_name, status_id, status_name, status_sort, 
                        company_id, company_identifier, company_name, severity, impact, customer_updated_flag, 
                        initial_description, initial_internal_analysis, initial_resolution, initial_description_from, 
                        process_notifications, skip_callback, closed_date, closed_by, closed_flag, actual_hours, approved, 
                        date_resolved, date_resplan, date_responded, resolve_minutes, res_plan_minutes, respond_minutes, 
                        is_in_sla, knowledge_base_link_id, resources, parent_ticket_id, has_child_ticket, 
                        has_merged_child_ticket_flag, knowledge_base_link_type, bill_time, bill_expenses, bill_products, 
                        lag_days, lag_nonworking_days_flag, estimated_start_date, duration, sla_status, 
                        request_for_change_flag, escalation_start_date_utc, escalation_level, minutes_before_waiting, 
                        responded_skipped_minutes, resplan_skipped_minutes, responded_hours, responded_by, resplan_hours, 
                        resplan_by, resolution_hours, resolved_by, minutes_waiting
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ''', (
                    ticket.id, clean_value(ticket.summary), 
                    clean_value(getattr(ticket.board, 'id', None)), clean_value(getattr(ticket.board, 'name', None)), 
                    clean_value(getattr(ticket.status, 'id', None)), clean_value(getattr(ticket.status, 'name', None)), 
                    clean_value(getattr(ticket.status, 'sort', None)), clean_value(getattr(ticket.company, 'id', None)), 
                    clean_value(getattr(ticket.company, 'identifier', None)), clean_value(getattr(ticket.company, 'name', None)), 
                    clean_value(ticket.severity), clean_value(ticket.impact), clean_value(getattr(ticket, 'customerUpdatedFlag', None)), 
                    clean_value(getattr(ticket, 'initialDescription', None)), clean_value(getattr(ticket, 'initialInternalAnalysis', None)), 
                    clean_value(getattr(ticket, 'initialResolution', None)), clean_value(getattr(ticket, 'initialDescriptionFrom', None)), 
                    clean_value(getattr(ticket, 'processNotifications', None)), clean_value(getattr(ticket, 'skipCallback', None)), 
                    convert_datetime(getattr(ticket, 'closedDate', None)), clean_value(getattr(ticket, 'closedBy', None)), 
                    clean_value(getattr(ticket, 'closedFlag', None)), clean_value(getattr(ticket, 'actualHours', None)), clean_value(getattr(ticket, 'approved', None)), 
                    convert_datetime(getattr(ticket, 'dateResolved', None)), convert_datetime(getattr(ticket, 'dateResplan', None)), 
                    convert_datetime(getattr(ticket, 'dateResponded', None)), clean_value(getattr(ticket, 'resolveMinutes', None)), 
                    clean_value(getattr(ticket, 'resPlanMinutes', None)), clean_value(getattr(ticket, 'respondMinutes', None)), clean_value(getattr(ticket, 'isInSla', None)), 
                    clean_value(getattr(ticket, 'knowledgeBaseLinkId', None)), clean_value(getattr(ticket, 'resources', None)), clean_value(getattr(ticket, 'parentTicketId', None)), 
                    clean_value(getattr(ticket, 'hasChildTicket', None)), clean_value(getattr(ticket, 'hasMergedChildTicketFlag', None)), 
                    clean_value(getattr(ticket, 'knowledgeBaseLinkType', None)), clean_value(getattr(ticket, 'billTime', None)), clean_value(getattr(ticket, 'billExpenses', None)), 
                    clean_value(getattr(ticket, 'billProducts', None)), clean_value(getattr(ticket, 'lagDays', None)), clean_value(getattr(ticket, 'lagNonworkingDaysFlag', None)), 
                    convert_datetime(getattr(ticket, 'estimatedStartDate', None)), clean_value(getattr(ticket, 'duration', None)), 
                    clean_value(getattr(ticket, 'slaStatus', None)), clean_value(getattr(ticket, 'requestForChangeFlag', None)), 
                    convert_datetime(getattr(ticket, 'escalationStartDateUTC', None)), clean_value(getattr(ticket, 'escalationLevel', None)), 
                    clean_value(getattr(ticket, 'minutesBeforeWaiting', None)), clean_value(getattr(ticket, 'respondedSkippedMinutes', None)), 
                    clean_value(getattr(ticket, 'resplanSkippedMinutes', None)), clean_value(getattr(ticket, 'respondedHours', None)), clean_value(getattr(ticket, 'respondedBy', None)), 
                    clean_value(getattr(ticket, 'resplanHours', None)), clean_value(getattr(ticket, 'resplanBy', None)), clean_value(getattr(ticket, 'resolutionHours', None)), 
                    clean_value(getattr(ticket, 'resolvedBy', None)), clean_value(getattr(ticket, 'minutesWaiting', None))
                ))
                conn.commit()
        except mysql.connector.Error as err:
            print(f"Error: {err}")
            continue
    
    if not paginated_service_tickets.has_next_page:
        break
    
    paginated_service_tickets.get_next_page()
    page_number += 1

conn.close()

import json
import csv
import boto3
import logging
import os
import time

# Enable logging
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# Response helpers
def elicit_slot(session_attributes, intent_name, slots, slot_to_elicit, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'ElicitSlot',
            'intentName': intent_name,
            'slots': slots,
            'slotToElicit': slot_to_elicit,
            'message': message
        }
    }

def close(session_attributes, fulfillment_state, message):
    return {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }

# Read employee data from S3 CSV
def get_employee_data(dept_id):
    try:
        s3 = boto3.client("s3")
        file_obj = s3.get_object(Bucket="demochatbotbucket", Key="employeeDetails.csv")
        lines = file_obj['Body'].read().decode('utf-8', errors='ignore').splitlines()
        reader = csv.reader(lines)
        header = next(reader, None)

        employees = []
        for row in reader:
            if row and len(row) >= 5 and row[0].strip() == dept_id.strip():
                employees.append({
                    'id': row[1].strip(),
                    'name': row[2].strip(),
                    'salary': row[3].strip(),
                    'location': row[4].strip()
                })
        return employees
    except Exception as e:
        logger.error(f"Error reading CSV from S3: {str(e)}", exc_info=True)
        return None

# Handle intent logic
def return_EmployeeName(intent_request):
    session_attributes = intent_request.get('sessionAttributes', {}) or {}
    slots = intent_request['currentIntent']['slots']
    Department_id = slots['DepartmentID']
    InformationType = slots['InformationType']

    # Validate DepartmentID
    if not Department_id or not Department_id.strip().isdigit():
        return elicit_slot(
            session_attributes,
            'ReturnEmployeeName',
            slots,
            'DepartmentID',
            {'contentType': 'PlainText', 'content': 'Please provide a valid numeric Department ID.'}
        )

    # Get employees from CSV
    employees = get_employee_data(Department_id)
    if not employees:
        return close(
            session_attributes,
            'Fulfilled',
            {'contentType': 'PlainText', 'content': 'No employees found in this department.'}
        )

    # Ask for info type if not provided
    if not InformationType:
        return elicit_slot(
            session_attributes,
            'ReturnEmployeeName',
            slots,
            'InformationType',
            {
                'contentType': 'PlainText',
                'content': 'Do you want ID, salary, or location along with employee names?'
            }
        )

    info_type = InformationType.strip().lower()
    response = []

    # Always include name; add extras if keywords exist
    for emp in employees[:10]:  # Show top 10 to stay within Lex limits
        parts = [f"Name: {emp['name']}"]

        if "id" in info_type:
            parts.append(f"ID: {emp['id']}")
        if "salary" in info_type:
            parts.append(f"Salary: {emp['salary']}")
        if "location" in info_type:
            parts.append(f"Location: {emp['location']}")

        response.append(" | ".join(parts))

    # Final message
    final_message = f"Found {len(employees)} employees. Showing top {len(response)}:\n" + "\n".join(response)

    # Ensure session attributes are strings (Lex requires this)
    session_attributes = {k: str(v) for k, v in session_attributes.items()}

    return close(
        session_attributes,
        'Fulfilled',
        {
            'contentType': 'PlainText',
            'content': final_message
        }
    )

# Intent dispatcher
def dispatch(intent_request):
    logger.debug(f"Dispatching intent: {intent_request['currentIntent']['name']}")
    intent_name = intent_request['currentIntent']['name']

    if intent_name == 'ReturnEmployeeName':
        return return_EmployeeName(intent_request)
    else:
        raise Exception(f'Intent {intent_name} not supported')

# Entry point
def lambda_handler(event, context):
    try:
        os.environ['TZ'] = 'America/New_York'
        time.tzset()
        logger.debug(f"Lambda triggered with event: {json.dumps(event)}")
        return dispatch(event)
    except Exception as e:
        logger.error(f"Unhandled error: {str(e)}", exc_info=True)
        return {
            'dialogAction': {
                'type': 'Close',
                'fulfillmentState': 'Failed',
                'message': {
                    'contentType': 'PlainText',
                    'content': 'An unexpected error occurred while processing your request.'
                }
            }
        }

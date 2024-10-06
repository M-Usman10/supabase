import os
import pandas as pd
import numpy as np
import uuid
from supabase import create_client, Client
import logging
import csv

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Supabase connection details
url = 'http://45.14.135.23:8000/'  # Replace with your Supabase project URL
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'  # Replace with your actual key

# Initialize Supabase client
supabase: Client = create_client(url, key)

def clean_string(value):
    """Helper function to safely strip strings and handle non-string values."""
    if isinstance(value, str):
        return value.strip()
    elif pd.isna(value):
        return None
    else:
        return str(value).strip()

def process_csv_files(folder_path):
    logging.info(f"Processing folder: {folder_path}")
    stats = {
        'total_files': 0,
        'total_records': 0,
        'successful_inserts': 0,
        'successful_updates': 0,
        'failed_records': 0,
    }
    for file_name in os.listdir(folder_path):
        if file_name.endswith('.csv'):
            stats['total_files'] += 1
            file_path = os.path.join(folder_path, file_name)
            logging.info(f"Processing file: {file_path}")
            process_single_csv(file_path, file_name, stats)
    # After processing all files, print the summary
    logging.info(f"Processing complete.")
    logging.info(f"Total files processed: {stats['total_files']}")
    logging.info(f"Total records processed: {stats['total_records']}")
    logging.info(f"Successful inserts: {stats['successful_inserts']}")
    logging.info(f"Successful updates: {stats['successful_updates']}")
    logging.info(f"Failed records: {stats['failed_records']}")

def process_single_csv(file_path, file_name, stats):
    try:
        # Fix the header line before reading the CSV
        with open(file_path, 'r', encoding='utf-8') as f:
            header_line = f.readline()
            rest_of_file = f

            # Correct the header line
            header_line = header_line.replace(
                'person_detailed_functioperson_title_normalized',
                'person_detailed_function\tperson_title_normalized'
            )

        # Define the correct column names
        expected_columns = [
            'person_name', 'person_first_name_unanalyzed', 'person_last_name_unanalyzed',
            'person_name_unanalyzed_downcase', 'person_title', 'person_functions',
            'person_seniority', 'person_email_status_cd', 'person_extrapolated_email_confidence',
            'person_email', 'person_phone', 'person_sanitized_phone', 'person_email_analyzed',
            'person_linkedin_url', 'person_detailed_function', 'person_title_normalized',
            'primary_title_normalized_for_faceting', 'sanitized_organization_name_unanalyzed',
            'person_location_city', 'person_location_city_with_state_or_country',
            'person_location_state', 'person_location_state_with_country', 'person_location_country',
            'person_location_postal_code', 'job_start_date', 'current_organization_ids',
            'modality', 'prospected_by_team_ids', 'person_excluded_by_team_ids', 'relavence_boost',
            'person_num_linkedin_connections', 'person_location_geojson', 'predictive_scores',
            'person_vacuumed_at', 'random', '_index', '_type', '_id', '_score'
        ]

        # Read the CSV file in chunks
        chunksize = 10000  # Adjust the chunk size as needed
        chunk_iter = pd.read_csv(
            file_path,
            delimiter='\t',
            engine='python',
            quoting=csv.QUOTE_NONE,
            on_bad_lines='skip',
            escapechar='\\',
            chunksize=chunksize,
            header=None,
            skiprows=1,  # Skip the original header
            names=expected_columns
        )

        total_records_in_file = 0

        for chunk in chunk_iter:
            total_records_in_file += len(chunk)
            process_chunk(chunk, file_name, stats)

        logging.info(f"File {file_name} processed successfully. Records: {total_records_in_file}")
        stats['total_records'] += total_records_in_file

    except Exception as e:
        logging.error(f"Error reading {file_name}: {e}")
        return

# Initialize a global email counter
global_email_counter = 0

def process_chunk(chunk, file_name, stats):
    global global_email_counter  # Declare as global to maintain state across chunks

    batch_size = 1000  # Adjust the batch size as needed

    # Prepare lists for batch processing
    emails = []
    data_rows = []
    email_to_row_map = {}

    # Iterate through the rows and collect data
    for index, row in chunk.iterrows():
        email = row.get('person_email', None)
        if pd.notna(email):
            email = clean_string(email)
            if email and '@' in email:  # Ensure email is valid after cleaning
                # Increment the global email counter
                global_email_counter += 1

                # Check if we've processed another 100,000 emails
                if global_email_counter % 100000 == 0:
                    logging.info(f"Processed {global_email_counter} emails so far.")

                emails.append(email)
                data = get_mapped_data(row, file_name)
                data_rows.append(data)
                email_to_row_map[email] = data
            else:
                logging.warning(f"Invalid email found in row {index}")
                stats['failed_records'] += 1
        else:
            logging.warning(f"No email found in row {index}")
            stats['failed_records'] += 1

    # Process in batches
    for i in range(0, len(emails), batch_size):
        batch_emails = emails[i:i+batch_size]
        batch_data = data_rows[i:i+batch_size]

        # Get existing records in batch
        existing_emails = get_existing_emails(batch_emails)

        # Separate data into inserts and updates
        records_to_insert = []
        records_to_update = []

        for data in batch_data:
            email = data['email']
            if email in existing_emails:
                # Prepare data for update
                existing_record = existing_emails[email]
                updated_data = {}
                for field, new_value in data.items():
                    if not existing_record.get(field) and new_value:
                        updated_data[field] = new_value
                if updated_data:
                    updated_data['email'] = email  # Ensure email is included for the update condition
                    records_to_update.append(updated_data)
                    stats['successful_updates'] += 1
            else:
                records_to_insert.append(data)
                stats['successful_inserts'] += 1

        # Insert new records
        if records_to_insert:
            insert_into_supabase(records_to_insert, stats)

        # Update existing records
        if records_to_update:
            update_supabase_records(records_to_update, stats)

def get_existing_emails(emails):
    existing_records = {}
    try:
        max_query_size = 50  # Adjust as needed to prevent URI too long errors
        for i in range(0, len(emails), max_query_size):
            batch = emails[i:i+max_query_size]
            # Build the criteria string for the 'in' operator
            email_list = [email.replace('"', '\\"') for email in batch]  # Escape double quotes
            criteria = '("' + '","'.join(email_list) + '")'
            response = supabase.table('contacts').select('*').filter('email', 'in', criteria).execute()
            if response.data:
                for record in response.data:
                    existing_records[record['email']] = record
    except Exception as e:
        logging.error(f"Error fetching existing records: {e}")
    return existing_records

def get_mapped_data(row, file_name):
    email = row.get('person_email', None)
    email = clean_string(email)
    domain = None
    if email and '@' in email:
        domain = email.split('@')[1]

    data = {
        'id': str(uuid.uuid4()),  # Generate a UUID for the 'id' field
        'email': email,
        'domain': clean_string(domain),
        'first_name': clean_string(row.get('person_first_name_unanalyzed', None)),
        'last_name': clean_string(row.get('person_last_name_unanalyzed', None)),
        'middle_name': '',  # Not available
        'full_name': clean_string(row.get('person_name', None)),
        'title_position': clean_string(row.get('person_title', None)),
        'company': clean_string(row.get('sanitized_organization_name_unanalyzed', None)),
        'job_title': clean_string(row.get('person_title', None)),
        'department': '',  # Not available
        'industry': '',  # Not available
        'email_status': clean_string(row.get('person_email_status_cd', None)),
        'phone_number': clean_string(row.get('person_phone', None)),
        'mobile_number': clean_string(row.get('person_sanitized_phone', None)),
        'address': '',  # Not available
        'city': clean_string(row.get('person_location_city', None)),
        'state': clean_string(row.get('person_location_state', None)),
        'country': clean_string(row.get('person_location_country', None)),
        'zip_code': clean_string(row.get('person_location_postal_code', None)),
        'website_url': '',  # Not available
        'linkedin': clean_string(row.get('person_linkedin_url', None)),
        'facebook': '',  # Not available
        'twitter': '',  # Not available
        'date_of_birth': '',  # Not available
        'gender': '',  # Not available
        'education_level': '',  # Not available
        'college_university': '',  # Not available
        'degree': '',  # Not available
        'certifications': '',  # Not available
        'years_of_experience': '',  # Not available
        'skills': '',  # Not available
        'interests': '',  # Not available
        'hobbies': '',  # Not available
        'languages_spoken': '',  # Not available
        'marital_status': '',  # Not available
        'company_salary': '',  # Not available
        'company_revenue': '',  # Not available
        'customer_segment': '',  # Not available
        'lead_source': '',  # Not available
        'lead_score': '',  # Not available
        'customer_lifetime_value': '',  # Not available
        'purchase_history': '',  # Not available
        'purchase_preferences': '',  # Not available
        'last_interacted_at': '',  # Not available
        'created_at': pd.Timestamp.now().isoformat(),
        'updated_at': pd.Timestamp.now().isoformat(),
        'file_name': file_name,
        'user_id': '',  # Not available
    }

    # Clean the data to ensure it's JSON-serializable
    clean_data = {}
    for key, value in data.items():
        if pd.isna(value) or value == '':
            clean_data[key] = None
        elif isinstance(value, (np.generic, np.number)):
            clean_data[key] = value.item()
        elif isinstance(value, (pd.Timestamp, pd.Timedelta, pd.Period)):
            clean_data[key] = str(value)
        else:
            clean_data[key] = value

    return clean_data

def update_supabase_records(records_to_update, stats):
    try:
        for data in records_to_update:
            email = data.pop('email')
            response = supabase.table('contacts').update(data).eq('email', email).execute()
            if not response.data:
                logging.error(f"Error updating record for {email}: {response.error}")
                stats['failed_records'] += 1
    except Exception as e:
        logging.error(f"Error updating records: {e}")

def insert_into_supabase(data_list, stats):
    try:
        response = supabase.table('contacts').insert(data_list).execute()
        if not response.data:
            if response.error and response.error['code'] == '23505':
                logging.warning(f"Duplicate record detected: {response.error['details']}")
                stats['failed_records'] += 1
            else:
                logging.error(f"Error inserting data: {response.error}")
                stats['failed_records'] += 1
    except Exception as e:
        logging.error(f"Error inserting data: {e}")
        stats['failed_records'] += 1

# Define the folder path where CSV files are stored
folder_path = '/home/data/Apollo/Apollo_BF/data/data/'  # Update with your actual path

# Process all files in the folder
process_csv_files(folder_path)
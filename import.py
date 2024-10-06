import os
import pandas as pd
import uuid
from supabase import create_client, Client
import logging
import smtplib
from email.mime.text import MIMEText
import csv  # Import csv module for QUOTE_NONE

# Initialize logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Supabase connection details
url = 'http://45.14.135.23:8000/'
key = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyAgCiAgICAicm9sZSI6ICJhbm9uIiwKICAgICJpc3MiOiAic3VwYWJhc2UtZGVtbyIsCiAgICAiaWF0IjogMTY0MTc2OTIwMCwKICAgICJleHAiOiAxNzk5NTM1NjAwCn0.dc_X5iR_VP_qT0zsiyj_I_OZ2T9FtRU2BBNWN8Bu4GE'  # Replace with your actual key

# Initialize Supabase client
supabase: Client = create_client(url, key)

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

    # Optionally send an email notification
    recipient_email = 'your_email@example.com'  # Replace with your email
    send_email_notification(stats, recipient_email)

def process_single_csv(file_path, file_name, stats):
    try:
        # Fix the header line before reading the CSV
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = f.readlines()
        # Correct the header line
        header_line = lines[0].replace('person_detailed_functioperson_title_normalized', 'person_detailed_function\tperson_title_normalized')
        # Write the corrected lines to a temporary file
        temp_file_path = file_path + '_temp'
        with open(temp_file_path, 'w', encoding='utf-8') as f:
            f.write(header_line)
            f.writelines(lines[1:])

        # Read the CSV file into a DataFrame with tab delimiter
        df = pd.read_csv(
            temp_file_path,
            delimiter='\t',
            engine='python',
            quoting=csv.QUOTE_NONE,
            error_bad_lines=False,  # For pandas <1.3
            # on_bad_lines='skip',  # For pandas >=1.3
            escapechar='\\',
            warn_bad_lines=True
        )
        # Remove the temporary file
        os.remove(temp_file_path)

        logging.info(f"File {file_name} read successfully. Records: {len(df)}")
        stats['total_records'] += len(df)

        # Provide the correct column names
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
        df.columns = expected_columns[:len(df.columns)]
    except Exception as e:
        logging.error(f"Error reading {file_name}: {e}")
        return

    # Iterate through the rows and prepare the data for Supabase
    for index, row in df.iterrows():
        email = row.get('person_email', None)
        if pd.notna(email):
            logging.info(f"Processing email: {email}")
            try:
                existing_record = get_record_by_email(email)
                if existing_record:
                    logging.info(f"Found existing record for {email}, updating...")
                    updated_data = {}
                    for field, new_value in get_mapped_data(row, file_name).items():
                        if not existing_record.get(field) and new_value:
                            updated_data[field] = new_value
                    if updated_data:
                        logging.info(f"Updating record for {email} with {updated_data}")
                        update_supabase_record(email, updated_data)
                        stats['successful_updates'] += 1
                    else:
                        logging.info(f"No fields to update for {email}")
                else:
                    logging.info(f"No existing record found for {email}, inserting new record.")
                    insert_into_supabase(get_mapped_data(row, file_name))
                    stats['successful_inserts'] += 1
            except Exception as e:
                logging.error(f"Error processing email {email}: {e}")
                stats['failed_records'] += 1
        else:
            logging.warning(f"No email found in row {index}")
            stats['failed_records'] += 1

def get_record_by_email(email):
    try:
        response = supabase.table('contacts').select('*').eq('email', email).execute()
        if response.data:
            return response.data[0]
        return None
    except Exception as e:
        logging.error(f"Error fetching record for {email}: {e}")
        return None

def get_mapped_data(row, file_name):
    email = row.get('person_email', None)
    domain = None
    if pd.notna(email) and '@' in email:
        domain = email.split('@')[1]

    return {
        'id': str(uuid.uuid4()),  # Generate a UUID for the 'id' field
        'email': email,
        'domain': domain,
        'first_name': row.get('person_first_name_unanalyzed', ''),
        'last_name': row.get('person_last_name_unanalyzed', ''),
        'middle_name': '',  # Not available
        'full_name': row.get('person_name', ''),
        'title_position': row.get('person_title', ''),
        'company': row.get('sanitized_organization_name_unanalyzed', ''),
        'job_title': row.get('person_title', ''),
        'department': '',  # Not available
        'industry': '',  # Not available
        'email_status': row.get('person_email_status_cd', ''),
        'phone_number': row.get('person_phone', ''),
        'mobile_number': row.get('person_sanitized_phone', ''),
        'address': '',  # Not available
        'city': row.get('person_location_city', ''),
        'state': row.get('person_location_state', ''),
        'country': row.get('person_location_country', ''),
        'zip_code': row.get('person_location_postal_code', ''),
        'website_url': '',  # Not available
        'linkedin': row.get('person_linkedin_url', ''),
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

def update_supabase_record(email, updated_data):
    try:
        response = supabase.table('contacts').update(updated_data).eq('email', email).execute()
        if response.data:
            logging.info(f"Successfully updated record for {email}")
        else:
            logging.error(f"Error updating record for {email}: {response}")
    except Exception as e:
        logging.error(f"Error updating record for {email}: {e}")

def insert_into_supabase(data):
    try:
        response = supabase.table('contacts').insert(data).execute()
        if response.data:
            logging.info(f"Successfully inserted new record for {data['email']}")
        else:
            logging.error(f"Error inserting data for {data['email']}: {response}")
    except Exception as e:
        logging.error(f"Error inserting data for {data['email']}: {e}")

def send_email_notification(stats, recipient_email):
    # Compose the email
    subject = "Contact Import Summary"
    body = f"""
    Contact Import Summary:

    Total files processed: {stats['total_files']}
    Total records processed: {stats['total_records']}
    Successful inserts: {stats['successful_inserts']}
    Successful updates: {stats['successful_updates']}
    Failed records: {stats['failed_records']}
    """
    msg = MIMEText(body)
    msg['Subject'] = subject
    msg['From'] = 'no-reply@example.com'  # Replace with your sender email
    msg['To'] = recipient_email

    # Send the email
    try:
        smtp_server = 'smtp.example.com'  # Replace with your SMTP server
        smtp_port = 587  # Replace with your SMTP port
        smtp_username = 'your_username'  # Replace with your SMTP username
        smtp_password = 'your_password'  # Replace with your SMTP password

        server = smtplib.SMTP(smtp_server, smtp_port)
        server.starttls()
        server.login(smtp_username, smtp_password)
        server.sendmail(msg['From'], [msg['To']], msg.as_string())
        server.quit()
        logging.info(f"Email notification sent to {recipient_email}")
    except Exception as e:
        logging.error(f"Failed to send email notification: {e}")

# Define the folder path where CSV files are stored
folder_path = '/home/data/Apollo/Apollo_BF/data/data/'  # Update with your actual path

# Process all files in the folder
process_csv_files(folder_path)

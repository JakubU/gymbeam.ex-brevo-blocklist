import os
import time
import requests
import pandas as pd
import logging
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configuration variables
KEY_API_TOKEN = '#api_token'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
KEY_TRANSACTIONAL = 'transactional'
KEY_MARKETING = 'marketing'

BREVO_TRANSACTIONAL_ENDPOINT = "https://api.brevo.com/v3/smtp/blockedContacts"
BREVO_MARKETING_ENDPOINT = "https://api.brevo.com/v3/contacts"


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def run(self):
        params = self.configuration.parameters
        self.api_token = params.get(KEY_API_TOKEN)
        self.start_date = params.get(KEY_START_DATE)
        self.end_date = params.get(KEY_END_DATE)
        self.transactional = params.get(KEY_TRANSACTIONAL)
        self.marketing = params.get(KEY_MARKETING)

        if not self.api_token:
            raise UserException("API token is missing in the configuration.")

        if self.transactional:
            blocked_contacts_df = self.get_blocked_contacts()
            self.save_to_csv(blocked_contacts_df, 'blocked_contacts.csv')
            self.write_state_file({"last_run": datetime.utcnow().isoformat()})
        else:
            logging.info("Transactional parameter is not set to true. Skipping the data fetch process for transactional contacts.")
        
        if self.marketing:
            marketing_contacts_df = self.get_marketing_contacts()
            self.save_to_csv(marketing_contacts_df, 'marketing_contacts.csv')
            self.write_state_file({"last_run": datetime.utcnow().isoformat()})
        else:
            logging.info("Marketing parameter is not set to true. Skipping the data fetch process for marketing contacts.")

    def get_total_records(self, headers, endpoint, segment_id=None):
        params = {"limit": 1, "offset": 0}
        if segment_id:
            params['segmentId'] = segment_id

        attempts = 3
        while attempts > 0:
            try:
                logging.info(f"Fetching total number of records from {endpoint} with params {params}")
                response = requests.get(endpoint, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                total_records = data.get('count', 0)
                logging.info(f"Total records to fetch: {total_records}")
                return total_records
            except requests.RequestException as e:
                logging.error(f"Error fetching total records: {e}")
                attempts -= 1
                if attempts > 0:
                    time.sleep(2 ** (3 - attempts))  # Exponential backoff
                else:
                    raise UserException(f"Error fetching total records after multiple attempts: {e}")

        return 0

    def get_blocked_contacts(self):
        headers = {"api-key": self.api_token, "accept": "application/json"}
        total_records = self.get_total_records(headers, BREVO_TRANSACTIONAL_ENDPOINT)
        batch_size = 100
        all_contacts = []

        total_records = 10000
        offsets = range(0, total_records, batch_size)
        stop_fetching = False

        with ThreadPoolExecutor(max_workers=60) as executor:
            futures = {executor.submit(self.fetch_contacts_batch, offset, batch_size, headers, BREVO_TRANSACTIONAL_ENDPOINT): offset for offset in offsets}
            for future in as_completed(futures):
                try:
                    contacts = future.result()
                    if not contacts:
                        logging.info(f"No more contacts to fetch at offset {future.key}. Stopping further requests.")
                        stop_fetching = True
                        break
                    all_contacts.extend(contacts)
                except Exception as e:
                    logging.error(f"Error fetching data: {e}")
                if stop_fetching:
                    break

        logging.info(f"Total fetched contacts: {len(all_contacts)}")
        df = pd.DataFrame(all_contacts)
        logging.info(f"Total contacts in DataFrame: {len(df)}")
        return df

    def get_marketing_contacts(self):
        headers = {"api-key": self.api_token, "accept": "application/json"}
        segment_id = 8
        total_records = self.get_total_records(headers, BREVO_MARKETING_ENDPOINT, segment_id)
        batch_size = 1000
        all_contacts = []

        total_records = 100000
        offsets = range(0, total_records, batch_size)
        stop_fetching = False

        with ThreadPoolExecutor(max_workers=60) as executor:
            futures = {executor.submit(self.fetch_contacts_batch, offset, batch_size, headers, BREVO_MARKETING_ENDPOINT, segment_id): offset for offset in offsets}
            for future in as_completed(futures):
                try:
                    contacts = future.result()
                    if not contacts:
                        logging.info(f"No more contacts to fetch at offset {future.key}. Stopping further requests.")
                        stop_fetching = True
                        break
                    all_contacts.extend(contacts)
                except Exception as e:
                    logging.error(f"Error fetching data: {e}")
                if stop_fetching:
                    break

        logging.info(f"Total fetched contacts: {len(all_contacts)}")
        df = pd.DataFrame(all_contacts, columns=['id', 'email', 'emailBlacklisted', 'smsBlacklisted', 'createdAt', 'modifiedAt'])
        logging.info(f"Total contacts in DataFrame: {len(df)}")
        return df

    def fetch_contacts_batch(self, offset, batch_size, headers, endpoint, segment_id=None):
        params = {"limit": batch_size, "offset": offset, "sort": "desc"}
        if segment_id:
            params['segmentId'] = segment_id

        attempts = 3
        while attempts > 0:
            try:
                logging.info(f"Fetching contacts from {endpoint} with params: {params}")
                response = requests.get(endpoint, headers=headers, params=params)
                response.raise_for_status()
                data = response.json()
                contacts = data.get('contacts', [])
                valid_contacts = [contact for contact in contacts if contact.get('email') is not None]
                logging.info(f"Fetched {len(valid_contacts)} valid contacts at offset {offset}")
                return valid_contacts
            except requests.RequestException as e:
                logging.error(f"Error fetching data: {response.status_code} {response.text}")
                attempts -= 1
                if attempts > 0:
                    time.sleep(2 ** (3 - attempts))  # Exponential backoff
                else:
                    logging.warning(f"Failed to fetch contacts at offset {offset} after multiple attempts")
                    return []

    def save_to_csv(self, df, file_path):
        output_directory = 'data/out/tables'
        os.makedirs(output_directory, exist_ok=True)
        full_path = os.path.join(output_directory, file_path)
        df.to_csv(full_path, index=False)
        logging.info(f"Data saved to {full_path}")

        table = self.create_out_table_definition(file_path, incremental=True)
        out_table_path = table.full_path
        logging.info(f"Output table definition created at {out_table_path}")
        

"""
Main entrypoint
"""

if __name__ == "__main__":
    try:
        comp = Component()
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)

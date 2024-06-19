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

BREVO_API_ENDPOINT = "https://api.brevo.com/v3/smtp/blockedContacts"


class Component(ComponentBase):
    """
    Extends base class for general Python components. Initializes the CommonInterface
    and performs configuration validation.

    For easier debugging the data folder is picked up by default from `../data` path,
    relative to working directory.

    If `debug` parameter is present in the `config.json`, the default logger is set to verbose DEBUG mode.
    """

    def __init__(self):
        super().__init__()
        self.setup_logging()

    def setup_logging(self):
        # Configure logging format and level
        logging.basicConfig(level=logging.INFO,
                            format='%(asctime)s - %(levelname)s - %(message)s')

    def run(self):
        """
        Main execution code
        """
        params = self.configuration.parameters
        self.api_token = params.get(KEY_API_TOKEN)
        self.start_date = params.get(KEY_START_DATE)
        self.end_date = params.get(KEY_END_DATE)

        if not self.api_token:
            raise UserException("API token is missing in the configuration.")

        # Fetch blocked contacts data from Brevo API
        blocked_contacts_df = self.get_blocked_contacts()

        # Create output table (Tabledefinition - just metadata)
        table = self.create_out_table_definition(
            'blocked_contacts.csv', incremental=True, primary_key=['email'])

        # Get file path of the table (data/out/tables/blocked_contacts.csv)
        out_table_path = table.full_path

        # Save data to CSV
        self.save_to_csv(blocked_contacts_df, out_table_path)

        # Save the state (if needed)
        self.write_state_file({"last_run": datetime.utcnow().isoformat()})

    def get_total_records(self, headers):
        """
        Fetch total number of blocked contacts
        """
        params = {
            "limit": 1,  # Fetch only one record to get total count
            "offset": 0,
            "sort": "desc"
        }
        logging.info(
            f"Fetching total number of records from {BREVO_API_ENDPOINT} with params {params}")
        response = requests.get(
            BREVO_API_ENDPOINT, headers=headers, params=params)
        if response.status_code != 200:
            logging.error(
                f"Error fetching total records: {response.status_code} {response.text}")
            raise UserException(
                f"Error fetching total records: {response.status_code} {response.text}")

        data = response.json()
        total_records = data.get('count', 0)
        logging.info(f"Total records to fetch: {total_records}")
        return total_records

    def get_blocked_contacts(self):
        """
        Fetch blocked contacts from Brevo API and return as DataFrame
        """
        headers = {
            "api-key": self.api_token,
            "accept": "application/json"
        }

        total_records = self.get_total_records(headers)
        batch_size = 100  # Fetch 100 records at a time
        all_contacts = []

        offsets = range(0, total_records, batch_size)
        stop_fetching = False

        with ThreadPoolExecutor(max_workers=100) as executor:
            futures = {executor.submit(
                self.fetch_contacts_batch, offset, batch_size, headers): offset for offset in offsets}
            for future in as_completed(futures):
                try:
                    contacts = future.result()
                    if not contacts:  # If no contacts were returned, stop further requests
                        logging.info(
                            f"No more contacts to fetch at offset {future.key}. Stopping further requests.")
                        stop_fetching = True
                        break
                    all_contacts.extend(contacts)
                except Exception as e:
                    logging.error(f"Error fetching data: {e}")
                if stop_fetching:
                    break

        # Log the number of records fetched
        logging.info(f"Total fetched contacts: {len(all_contacts)}")

        # Remove duplicates if any
        df = pd.DataFrame(all_contacts).drop_duplicates(subset='email')
        logging.info(
            f"Total unique contacts after removing duplicates: {len(df)}")

        return df

    def fetch_contacts_batch(self, offset, batch_size, headers):
        """
        Fetch a batch of blocked contacts from Brevo API
        """
        params = {
            "limit": batch_size,
            "offset": offset,
            "sort": "desc"
        }
        attempts = 3
        while attempts > 0:
            logging.info(
                f"Fetching contacts from {BREVO_API_ENDPOINT} with params: {params}")
            response = requests.get(
                BREVO_API_ENDPOINT, headers=headers, params=params)
            if response.status_code == 200:
                data = response.json()
                contacts = data.get('contacts', [])
                # Filter out contacts without email
                valid_contacts = [
                    contact for contact in contacts if contact.get('email') is not None]
                logging.info(
                    f"Fetched {len(valid_contacts)} valid contacts at offset {offset}")
                return valid_contacts
            elif response.status_code == 404:
                # Stop retrying if a 404 is encountered
                logging.warning(
                    f"Offset {offset} returned 404 Not Found, stopping retries.")
                break
            else:
                logging.error(
                    f"Error fetching data: {response.status_code} {response.text}")
                attempts -= 1
                logging.info(f"Retrying... {3 - attempts} of 3 attempts left")

        logging.warning(
            f"Failed to fetch contacts at offset {offset} after 3 attempts")
        return []

    def save_to_csv(self, df, file_path):
        """
        Save DataFrame to CSV
        """
        df.to_csv(file_path, index=False)
        logging.info(f"Data saved to {file_path}")


"""
Main entrypoint
"""


if __name__ == "__main__":
    try:
        comp = Component()
        # This triggers the run method by default and is controlled by the configuration.action parameter
        comp.execute_action()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)

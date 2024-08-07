import time
import requests
import pandas as pd
import logging
from datetime import datetime
from keboola.component.base import ComponentBase
from keboola.component.exceptions import UserException
import queue
import gc
import aiohttp
import asyncio
from keboola.component import CommonInterface


# Configuration variables
KEY_API_TOKEN = '#api_token'
KEY_START_DATE = 'start_date'
KEY_END_DATE = 'end_date'
KEY_TRANSACTIONAL = 'transactional'
KEY_MARKETING = 'marketing'

BREVO_TRANSACTIONAL_ENDPOINT = "https://api.brevo.com/v3/smtp/blockedContacts"
BREVO_MARKETING_ENDPOINT = "https://api.brevo.com/v3/contacts"

# Set the data directory for local testing
# if not os.path.exists('/data/'):
#    os.environ['KBC_DATADIR'] = './data'


class Component(ComponentBase):
    def __init__(self):
        super().__init__()
        self.setup_logging()
        self.ci = CommonInterface()

    def setup_logging(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

    def run(self):
        logging.info("Starting the component run process")
        params = self.configuration.parameters
        self.api_token = params.get(KEY_API_TOKEN)
        self.start_date = params.get(KEY_START_DATE)
        self.end_date = params.get(KEY_END_DATE)
        self.transactional = params.get(KEY_TRANSACTIONAL)
        self.marketing = params.get(KEY_MARKETING)

        if not self.api_token:
            raise UserException("API token is missing in the configuration.")

        if self.transactional:
            logging.info("Starting to fetch transactional contacts")
            self.get_blocked_contacts()
            self.write_state_file({"last_run": datetime.utcnow().isoformat()})
            logging.info("Completed fetching transactional contacts")
        else:
            logging.info("Transactional parameter is not set to true. Skipping the data fetch process for transactional contacts.")
        if self.marketing:
            logging.info("Starting to fetch marketing contacts")
            self.get_marketing_contacts()
            self.write_state_file({"last_run": datetime.utcnow().isoformat()})
            logging.info("Completed fetching marketing contacts")
        else:
            logging.info("Marketing parameter is not set to true. Skipping the data fetch process for marketing contacts.")

        logging.info("Completed the component run process")

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

    async def fetch_contacts_batch(self, session, offset, batch_size, headers, endpoint, segment_id=None):
        params = {"limit": batch_size, "offset": offset, "sort": "desc"}
        if segment_id:
            params['segmentId'] = segment_id

        max_attempts = 10  # Increased number of attempts
        for attempt in range(max_attempts):
            try:
                logging.info(f"Fetching contacts from {endpoint} with params {params} (Attempt {attempt + 1}/{max_attempts})")
                async with session.get(endpoint, headers=headers, params=params) as response:
                    response.raise_for_status()
                    data = await response.json()
                    contacts = data.get('contacts', [])
                    valid_contacts = [contact for contact in contacts if contact.get('email') is not None]
                    logging.info(f"Fetched {len(valid_contacts)} valid contacts at offset {offset}")
                    return valid_contacts
            except Exception as e:
                logging.error(f"Error fetching data on attempt {attempt + 1}: {e}")
                if attempt < max_attempts - 1:
                    await asyncio.sleep(2 ** attempt)  # Exponential backoff
                else:
                    logging.warning(f"Failed to fetch contacts at offset {offset} after {max_attempts} attempts")
                    return []

    async def process_batches(self, headers, endpoint, batch_size, total_records, output_file_path, segment_id=None, columns=None):
        offsets = queue.Queue()
        for offset in range(0, total_records, batch_size):
            offsets.put(offset)

        async def worker():
            async with aiohttp.ClientSession() as session:
                while not offsets.empty():
                    offset = offsets.get()
                    logging.info(f"Processing batch at offset {offset}")
                    contacts = await self.fetch_contacts_batch(session, offset, batch_size, headers, endpoint, segment_id)
                    if contacts:
                        logging.info(f"Fetched {len(contacts)} contacts at offset {offset}")
                        if endpoint == BREVO_TRANSACTIONAL_ENDPOINT:
                            # Flatten 'reason' dictionary into separate columns
                            for contact in contacts:
                                if 'reason' in contact:
                                    contact['reason_message'] = contact['reason'].get('message')
                                    contact['reason_code'] = contact['reason'].get('code')
                                    del contact['reason']
                            df = pd.DataFrame(contacts, columns=['email', 'reason_message', 'reason_code', 'blockedAt', 'senderEmail'])
                        else:
                            df = pd.DataFrame(contacts)
                            if columns:
                                df = df[columns]

                        logging.info(f"Writing batch to CSV at offset {offset}")
                        df.to_csv(output_file_path, mode='a', header=False, index=False)
                        del df
                        del contacts
                        gc.collect()
                        logging.info(f"Completed processing batch at offset {offset}")
                    else:
                        logging.info(f"No contacts fetched at offset {offset}")
                    offsets.task_done()

        logging.info("Starting threads for batch processing")
        tasks = [worker() for _ in range(30)]
        await asyncio.gather(*tasks)
        logging.info("All batches processed")

    def get_blocked_contacts(self):
        logging.info("Fetching transactional contacts - Initializing")
        headers = {"api-key": self.api_token, "accept": "application/json"}
        total_records = self.get_total_records(headers, BREVO_TRANSACTIONAL_ENDPOINT)
        batch_size = 100  # Adjust batch size as needed

        transactional_file_path = self.create_out_table_definition('transactional_contacts.csv', incremental=True).full_path
        with open(transactional_file_path, 'w') as f:
            f.write(','.join(['email', 'reason_message', 'reason_code', 'blockedAt', 'senderEmail']) + '\n')

        asyncio.run(self.process_batches(headers, BREVO_TRANSACTIONAL_ENDPOINT, batch_size, total_records, transactional_file_path))
        logging.info("Fetching transactional contacts - Completed")

    def get_marketing_contacts(self):
        logging.info("Fetching marketing contacts - Initializing")
        headers = {"api-key": self.api_token, "accept": "application/json"}
        segment_id = 8
        total_records = self.get_total_records(headers, BREVO_MARKETING_ENDPOINT, segment_id)
        batch_size = 1000  # Adjust batch size as needed

        marketing_file_path = self.create_out_table_definition('marketing_contacts.csv', incremental=True).full_path
        with open(marketing_file_path, 'w') as f:
            f.write(','.join(['id', 'email', 'emailBlacklisted', 'smsBlacklisted', 'createdAt', 'modifiedAt']) + '\n')

        asyncio.run(self.process_batches(headers, BREVO_MARKETING_ENDPOINT, batch_size, total_records, marketing_file_path, segment_id, columns=['id', 'email', 'emailBlacklisted', 'smsBlacklisted', 'createdAt', 'modifiedAt']))
        logging.info("Fetching marketing contacts - Completed")


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

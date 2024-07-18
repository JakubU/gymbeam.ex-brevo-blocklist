# Brevo - BlockList

## Description

This component fetches and processes marketing and transactional contacts data from the Brevo API.

**Table of contents:**

- [Functionality notes](#functionality-notes)
- [Prerequisites](#prerequisites)
- [Features](#features)
- [Supported endpoints](#supported-endpoints)
- [Configuration](#configuration)
- [Important Parameters](#important-parameters)
- [Fetching Process](#fetching-process)
- [Output](#output)
- [Development](#development)
- [Integration](#integration)

## Functionality notes

This component fetches and processes marketing and transactional contacts data from the Brevo API.

## Prerequisites

- Get the API token.
- Register the application with the Brevo API.

## Features

| **Feature**             | **Note**                                      |
|-------------------------|-----------------------------------------------|
| Generic UI form         | Dynamic UI form                               |
| Row Based configuration | Allows structuring the configuration in rows. |
| oAuth                   | oAuth authentication enabled                  |
| Incremental loading     | Allows fetching data in new increments.       |
| Backfill mode           | Support for seamless backfill setup.          |
| Date range filter       | Specify date range.                           |

## Supported endpoints

If you need more endpoints, please submit your request to [ideas.keboola.com](https://ideas.keboola.com/).

## Configuration

The component configuration is defined in `config.json` and includes the following main sections:

- `storage`: Defines input and output table settings.
- `parameters`: Contains API token and flags for transactional and marketing data fetching.
- `action`: Specifies the action to run (default is "run").
- `authorization`: Contains OAuth API credentials.

### Important Parameters

- **`#api_token`**: Your API token for authenticating with the Brevo API.
- **`transactional`**: Boolean flag indicating whether to fetch transactional contacts.
- **`marketing`**: Boolean flag indicating whether to fetch marketing contacts.

## Fetching Process

### Fetching Transactional Contacts

- Fetches blocked contacts from the Brevo API.
- Saves the data in the `transactional_contacts.csv` file.
- Columns include: `email`, `reason_message`, `reason_code`, `blockedAt`, `senderEmail`.

### Fetching Marketing Contacts

- Fetches marketing contacts from the Brevo API.
- Limits the total records to a maximum of 30,000.
- Saves the data in the `marketing_contacts.csv` file.
- Columns include: `id`, `email`, `emailBlacklisted`, `smsBlacklisted`, `createdAt`, `modifiedAt`.

### Merging Data

- If an input table is provided, it merges the new data with the old data using the `email` column.
- Updates the `blacklisted_timestamp` column if it exists; otherwise, it adds this column and fills missing values with the current timestamp.
- Ensures that the new data has at least 90% of the records compared to the old data; otherwise, it uses the old data as output.

## Output

The component outputs two CSV files:

- `transactional_contacts.csv`: Contains fetched blocked contacts.
  - Columns: `email`, `reason_message`, `reason_code`, `blockedAt`, `senderEmail`.
- `marketing_contacts.csv`: Contains fetched marketing contacts.
  - Columns: `id`, `email`, `emailBlacklisted`, `smsBlacklisted`, `createdAt`, `modifiedAt`, `blacklisted_timestamp`.

Development
-----------

If required, change local data folder (the `CUSTOM_FOLDER` placeholder) path to
your custom path in the `docker-compose.yml` file:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    volumes:
      - ./:/code
      - ./CUSTOM_FOLDER:/data
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Clone this repository, init the workspace and run the component with following
command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
git clone https://github.com/JakubU/gymbeam.ex-brevo-blocklist gymbeam.ex-brevo-blocklist
cd gymbeam.ex-brevo-blocklist
docker-compose build
docker-compose run --rm dev
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Run the test suite and lint check using this command:

~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
docker-compose run --rm test
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Integration
===========

For information about deployment and integration with KBC, please refer to the
[deployment section of developers
documentation](https://developers.keboola.com/extend/component/deployment/)

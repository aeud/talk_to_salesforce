# Salesforce Object Creation Script

## Overview

The Salesforce Object Creation Script is a Python tool designed to facilitate the creation or updating of Salesforce objects using data from various sources such as local files, Google Cloud Storage, or BigQuery. It provides a flexible and customizable solution for integrating external data with Salesforce, automating the process of data importation and manipulation.

## Features

- Seamless integration with Salesforce API: The script interacts with the Salesforce API to create or update Salesforce objects based on provided data.
- Support for multiple data sources: Data can be fetched from local files (CSV format), Google Cloud Storage buckets, or BigQuery queries, allowing users to choose the most suitable source for their needs.
- Customizable data transformation: Users can define custom Jinja templates to transform input data into the format expected by Salesforce, providing flexibility in data processing.
- Dry-run mode: The script offers a dry-run mode where HTTP requests are simulated, allowing users to preview the actions that would be taken without actually making changes to Salesforce data.
- Secure handling of credentials: The script supports fetching sensitive credentials from environment variables or Google Cloud Secret Manager, ensuring secure authentication with Salesforce API.

## Usage

### Installation

1. Clone the repository to your local machine.
2. Ensure that Python 3.x is installed on your system.
3. Install the required dependencies by running `pip install -r requirements.txt`.

### Running the Script

The script is executed from the command line using the `python` command followed by the script filename (`main.py`). Below is the general syntax:

```
python main.py [OPTIONS]
```


### Options

- `--input`: Specifies the source of input data. Supported formats include `gs://{bucket}/{key}` for Google Cloud Storage, `file://{local_path}` for local files, or `bigquery://{base64_encoded_sql_query}` for BigQuery queries. Use the `project` query parameter to define a specific GCP project to use for querying or loading data.
- `--input-file-format`: (Optional) Specifies the format of input files (default: CSV).
- `--input-csv-file-has-headers`: (Optional) Indicates whether the input CSV file contains headers.
- `--sf-api-req-item-json-template`: (Optional) Jinja template to transform input data into the format expected by Salesforce.
- `--sf-api-instance-url`: URL of the Salesforce instance.
- `--sf-api-access-token`: Access token used to authenticate the request.
- `--sf-api-object`: Salesforce object to insert or upsert.
- `--sf-api-external-id`: (Optional) To be used when upserting. Defines which attribute to use as a joining key.
- `--sf-api-bulk-size`: (Optional) Size of the bulk (default: 200).
- `--sf-api-all-or-none`: (Optional) Boolean used in the API request body.
- `--dry-run`: (Optional) Simulates HTTP requests without making changes to Salesforce data.

### Examples

1. Import data from a local CSV file and send it to Salesforce:

```
python main.py --input=file:///path/to/local/file.csv
--sf-api-access-token=your_access_token
--sf-api-instance-url=https://your-instance.salesforce.com
--sf-api-object=Account
```

2. Fetch rows from a BigQuery query and update Salesforce objects:

```
python main.py --input=bigquery://U29tZSBiaW5kIHF1ZXN0aW9ucyBhc3N1bXB0aW9u?project=xxx
--sf-api-instance-url=https://your-instance.salesforce.com
--sf-api-access-token=your_access_token
--sf-api-object=Account
--sf-api-external-id=customer_id
```


## Notes

- Ensure that you have appropriate permissions and access rights to the specified data sources and Salesforce instance.
- Review the Salesforce API documentation for the correct endpoint URLs and required request formats.
- Use the dry-run mode for testing and validation purposes before performing actual data operations.

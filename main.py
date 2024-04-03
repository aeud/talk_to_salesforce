"""Script used to create or update Saleforce objects from data sets"""

import csv
import json
import os
import re
import click
import uuid
import requests
import base64
import logging

# Setup logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(format='%(name)s - %(levelname)s - %(message)s', level=logging.INFO)

# Temporary file path for storing downloaded files
TMP_FILE_PATH = "/tmp/%s.csv" % str(uuid.uuid4())

# Constant for defining file format as CSV
FILE_FORMAT_CSV = "CSV"

class SaleforceAPIClient:
    """SaleforceBulkAPIClient is a tool we can use to interact with the Salesforce API.
    It contains the credentials, the logic to encode the requests, to interpret the responses.
    """

    def __init__(self, instance_url, endpoint, access_token, method="POST"):
        """Initializes a new instance of the SaleforceAPIClient class.

        Args:
            instance_url (str): URL of the instance that the org lives on.
            endpoint (str): Salesforce API endpoint to send the records to.
            access_token (str): Access token used to authenticate the request.
            method (str, optional): Method used to send the request with. Defaults to "POST".
        """
        self.instance_url = instance_url
        self.endpoint = endpoint
        self.method = method
        self.access_token = access_token
        self.bulk_size = 200  # default value
        self.queue = []
        self.queue_size = 0
        self.req_item_json_template = None
        self.dry_run_mode = False
        self.set_session(requests.Session())
        self.auth_session()
        self.reset_queue()

    def set_dry_run_mode(self, dry_run_mode):
        """Sets the dry run mode.

        Args:
            dry_run_mode (bool): True if dry run mode is enabled, False otherwise.
        """
        self.dry_run_mode = dry_run_mode

    def set_all_or_none(self, all_or_none):
        """Sets the 'allOrNone' parameter in the API request body.

        Args:
            all_or_none (bool): Boolean used in the API request body.
        """
        self.all_or_none = all_or_none

    def set_bulk_size(self, n):
        """Sets the size of the bulk.

        Args:
            n (int): Size of the bulk (number of records to be sent in a single API request body).
        """
        self.bulk_size = n

    def set_session(self, custom_session):
        """Sets a custom session for making HTTP requests.

        Args:
            custom_session (requests.Session): Custom session object.
        """
        self.session = custom_session

    def set_req_item_json_template(self, template):
        """Sets a Jinja template to convert rows or objects to a Salesforce expected input.

        Args:
            template (str): Jinja template string.
        """
        from jinja2 import Environment, BaseLoader

        self.req_item_json_template = Environment(loader=BaseLoader()).from_string(
            template
        )

    def reset_queue(self):
        """Resets the queue and its size to empty."""
        self.queue = []
        self.queue_size = 0

    def custom_json_encoder(self, row):
        """Encodes a given row using a custom JSON encoder.

        Args:
            row (dict): Row to be encoded.

        Returns:
            dict: Encoded row.
        """
        if self.req_item_json_template is not None:
            p = self.req_item_json_template.render({"row": row})
            try:
                v = json.loads(p)
            except json.decoder.JSONDecodeError as e:
                raise Exception("item template input is not properly JSON formatted", e)
            except Exception as e:
                raise Exception(e)
            return v
        return dict(row)

    def send_single_request(self, record, all_or_none=False):
        """Sends a single API request with the provided record as the payload.

        Args:
            record (dict): Record to be sent.
            all_or_none (bool, optional): Whether all records should be processed if any fail. Defaults to False.
        """
        return self.send_bulk_request(records=[record], all_or_none=all_or_none)

    def send_bulk_request(self, records):
        """Sends a bulk API request with the provided records as the payload.

        Args:
            records (list): List of records to be sent.
        """
        body = json.dumps(
            {
                "allOrNone": self.all_or_none,
                "records": records,
            }
        )
        self.send_http_request(self.method, body)

    def queue_item_and_send_bulk_request(self, item):
        """Adds an item to the queue and sends a bulk request if the queue size reaches the set bulk size.

        Args:
            item (dict): Item to be added to the queue.
        """
        self.queue.append(item)
        self.queue_size = self.queue_size + 1
        if self.queue_size >= self.bulk_size:
            self.send_bulk_request(self.queue)
            self.reset_queue()

    def send_all_it(self, it, bulk=True):
        """Sends all items in the iterable 'it', either individually or in bulk depending on the 'bulk' flag.

        Args:
            it (iterable): Iterable containing items to be sent.
            bulk (bool, optional): Whether to send items in bulk. Defaults to True.
        """
        for row in it:
            v = self.custom_json_encoder(row)
            if bulk:
                self.queue_item_and_send_bulk_request(v)
            else:
                self.send_single_request(v)
        self.flush()

    def flush(self):
        """Flushes the queue by sending any remaining items in bulk."""
        if self.queue_size > 0:
            self.send_bulk_request(self.queue)

    def auth_session(self):
        """Authenticates the session by adding necessary headers for authorization."""
        self.session.headers.update(
            {
                "Authorization": "Bearer %s" % self.access_token,
                "Content-Type": "application/json",
            }
        )
        pass

    def send_http_request(self, method="POST", body=None):
        """Sends an HTTP request to the Salesforce API service.

        Args:
            method (str, optional): HTTP method used to send the request. Defaults to "POST".
            body (str, optional): Body content of the HTTP request. Defaults to None.
        """
        url = "%s%s" % (self.instance_url, self.endpoint)
        logger.info(
            "Sending a %s HTTP request to %s"
            % (
                self.method,
                url,
            )
        )
        if self.dry_run_mode:
            logger.info("[DRY RUN] Would have sent: %s" % body)
            return
        req = requests.Request(method, url, data=body)
        prepped = self.session.prepare_request(req)
        if body is not None:
            prepped.body = body
        try:
            resp = self.session.send(prepped)
        except Exception as e:
            logger.info(e)
        if resp.status_code == 200:
            logger.info("request sent", resp.content.decode("utf-8"))
        else:
            logger.warning("error when sending the rows", resp.content)


def download_file(project_id, bucket_name, blob_name, tmp_file_path):
    """Downloads a file from Google Cloud Storage.

    Args:
        project_id (str): Google Cloud Storage project ID.
        bucket_name (str): Name of the bucket containing the file.
        blob_name (str): Name of the file to download.
        tmp_file_path (str): Temporary file path to save the downloaded file.
    """
    if project_id is None:
        raise Exception("Storage project cannot be null. Add it as an input parameter.")
    from google.cloud.storage import Client

    logger.info(
        "Downloading the file bucket: `%s` and key: `%s` (via project `%s`)..."
        % (
            bucket_name,
            blob_name,
            project_id,
        )
    )
    client = Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(tmp_file_path)
    logger.info("Content downloaded in file `%s`" % (tmp_file_path,))


def collect_rows(file, input_file_format, input_csv_file_has_headers=False):
    """Collects rows from a file.

    Args:
        file (file object): File object to read rows from.
        input_file_format (str): File format (currently only supports CSV).
        input_csv_file_has_headers (bool, optional): Whether the CSV file contains headers. Defaults to False.

    Returns:
        iterable: Iterable containing rows from the file.
    """
    if input_file_format == FILE_FORMAT_CSV:
        if input_csv_file_has_headers:
            reader = csv.DictReader(file)
        else:
            reader = csv.reader(file)
        return reader
    else:
        raise Exception("only CSV file format is developed yet")


def remove_tmp_file(tmp_file_path):
    """Removes a temporary file.

    Args:
        tmp_file_path (str): Path of the temporary file to be removed.
    """
    try:
        os.remove(tmp_file_path)
        logger.info("File `%s` deleted" % (tmp_file_path,))
    except OSError:
        pass


def args_secret_wrapper(a):
    """Wraps arguments to handle secrets or environment variables and fetches their values accordingly.

    Args:
        a (str): Argument to be wrapped.

    Returns:
        str: Secret or environment variable value.
    """
    if type(a) == str and a.startswith("env://"):
        var_name = a.replace("env://", "")
        val = os.getenv(var_name)
        if val is None:
            raise Exception("environment / secret value %s cannot be null" % var_name)
        return val
    if type(a) == str and re.match(r"^secretmanager\:\/\/projects\/\d+\/secrets\/", a):
        from google.cloud import secretmanager

        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(
            request={"name": a.replace("secretmanager://", "")}
        )
        val = response.payload.data.decode("UTF-8")
        if val is None:
            raise Exception("environment / secret value %s cannot be null" % var_name)
        return val
    return a


def storage_parse_path(path):
    """Parses a Google Cloud Storage path.

    Args:
        path (str): Google Cloud Storage path to be parsed.

    Returns:
        tuple: Tuple containing bucket name and blob name.
    """
    if not path.startswith("gs://"):
        raise Exception("Input path must start with gs://")
    elements = path.replace("gs://", "").split("/")
    return elements[0], "/".join(elements[1:])


def bigquery_parse_input(path):
    """Parses a BigQuery input path.

    Args:
        path (str): BigQuery input path containing a base64 encoded SQL query.

    Returns:
        str: Decoded SQL query.
    """
    if not path.startswith("bq://"):
        raise Exception("Input path must start with bq://")
    sql = base64.decodebytes(path.replace("bq://", "").encode("utf-8")).decode("utf-8")
    return sql


def get_rows(project_id, sql):
    """Fetches rows from BigQuery.

    Args:
        project_id (str): BigQuery project ID.
        sql (str): SQL query to fetch rows.

    Returns:
        iterable: Iterable containing rows fetched from BigQuery.
    """
    if project_id is None:
        raise Exception(
            "BigQuery project cannot be null. Add it as an input parameter."
        )
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    logger.info(
        "Fetching data from BigQuery (via project `%s`) using the script:\n%s"
        % (
            project_id,
            sql,
        )
    )
    query_job = client.query(sql)
    return query_job.result()


@click.command()
@click.option(
    "--input-path",
    help="This argument represents where / how the input will be fetched. It must be using one of the options: gs://{bucket}/{key}, file://{local_path} or bigquery://{base64_encoded_sql_query}",
    required=True,
)
@click.option(
    "--input-storage-project-id",
    help="Google Cloud Storage to use (billing) when fetching files using Storage",
)
@click.option(
    "--input-bigquery-project-id",
    help="Google Cloud Storage to use (billing) when fetching rows using BigQuery",
)
@click.option(
    "--input-file-format",
    default="CSV",
    help="If input is a file, precise the file format. Only CSV is available so far",
)
@click.option(
    "--input-csv-file-has-headers",
    default=False,
    is_flag=True,
    show_default=True,
    help="If input is a file, and if the file format is CSV, precise wether the file contains a header or not (first row)",
)
@click.option(
    "--sf-api-req-item-json-template",
    help="Jinja template to use to convert rows or objects to a Salesforce expected input. See documentation examples",
    default=None,
)
@click.option(
    "--sf-api-instance-url",
    help="URL of the instance that the org lives on",
    required=True,
)
@click.option(
    "--sf-api-endpoint",
    help="Salesforce API endpoint to send the records to",
    required=True,
)
@click.option(
    "--sf-api-access-token",
    help="Access token used to authenticate the request",
    required=True,
)
@click.option(
    "--sf-api-method",
    help="Method used to send the request with",
    default="POST",
)
@click.option(
    "--sf-api-bulk-size",
    default="200",
    help="Size of the bulk (number of records to be sent in a single API request body)",
)
@click.option(
    "--sf-api-all-or-none",
    default=False,
    is_flag=True,
    show_default=True,
    help="Boolean used in the API request body",
)
@click.option(
    "--dry-run",
    default=False,
    is_flag=True,
    show_default=True,
    help="When used, HTTP requests will not be sent, and the body will be written to standard output",
)
def main(
    input_path,
    input_storage_project_id,
    input_bigquery_project_id,
    input_file_format,
    input_csv_file_has_headers,
    sf_api_req_item_json_template,
    sf_api_instance_url,
    sf_api_endpoint,
    sf_api_access_token,
    sf_api_method,
    sf_api_bulk_size,
    sf_api_all_or_none,
    dry_run,
):
    """Main function that orchestrates the data fetching and sending to Salesforce."""

    # Check secrets or environment variables for relevant variables
    # Variable will use an environment variable when the arg starts with env:// (ex: env://SOME_ENV_VAR_NAME)
    # Variable will use the secret manager when the arg starts with secretmanager:// (ex: secretmanager://SOME_ENV_VAR_NAME)
    sf_api_instance_url = args_secret_wrapper(sf_api_instance_url)
    sf_api_access_token = args_secret_wrapper(sf_api_access_token)
    sf_api_bulk_size = int(args_secret_wrapper(sf_api_bulk_size))

    # Initialize Salesforce API client
    sf_client = SaleforceAPIClient(
        sf_api_instance_url, sf_api_endpoint, sf_api_access_token, method=sf_api_method
    )
    sf_client.set_all_or_none(sf_api_all_or_none)
    sf_client.set_bulk_size(sf_api_bulk_size)
    sf_client.set_dry_run_mode(dry_run)
    if sf_api_req_item_json_template is not None:
        sf_client.set_req_item_json_template(sf_api_req_item_json_template)

    # Fetch data based on input path and send to Salesforce
    if input_path.startswith("gs://"):
        input_bucket_name, input_blob_name = storage_parse_path(input_path)
        download_file(
            input_storage_project_id, input_bucket_name, input_blob_name, TMP_FILE_PATH
        )
        input_path = "file://%s" % TMP_FILE_PATH

    if input_path.startswith("file://"):
        input_file_path = input_path.replace("file://", "")
        file = open(input_file_path)
        it = collect_rows(
            file,
            input_file_format,
            input_csv_file_has_headers=input_csv_file_has_headers,
        )
    elif input_path.startswith("bq://"):
        sql = bigquery_parse_input(input_path)
        it = get_rows(input_bigquery_project_id, sql)
    else:
        raise Exception(
            "Input file path format must be either file:// (file system) or gs:// (Google Cloud Storage)"
        )

    sf_client.send_all_it(it, bulk=True)

    remove_tmp_file(TMP_FILE_PATH)


if __name__ == "__main__":
    main()

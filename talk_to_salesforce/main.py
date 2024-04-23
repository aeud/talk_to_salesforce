"""Script used to create or update Saleforce objects from data sets"""

import click
import logging
from talk_to_salesforce.src.salesforce import SaleforceAPIClient
from talk_to_salesforce.src.datasets import Dataset
from talk_to_salesforce.src.utils import args_secret_wrapper

# Setup logging configuration
logger = logging.getLogger(__name__)
logging.basicConfig(format="%(name)s - %(levelname)s - %(message)s", level=logging.INFO)


@click.command()
@click.option(
    "--input",
    help="This argument represents where / how the input will be fetched. It must be using one of the options: gs://{bucket}/{key}, file://{local_path} or bigquery://{base64_encoded_sql_query}, if a project is to be used, the project parameter should be used (using the query parameter convention)",
    required=True,
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
    "--sf-api-access-token",
    help="Access token used to authenticate the request",
    required=True,
)
@click.option(
    "--sf-api-object",
    help="Object name as referenced in Salesforce Object Manager",
    required=True,
)
@click.option(
    "--sf-api-external-id",
    help="External ID to use when upserting elements. When mentioned, the API will be called using the PATCH method",
    required=False,
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
    input,
    input_file_format,
    input_csv_file_has_headers,
    sf_api_req_item_json_template,
    sf_api_instance_url,
    sf_api_object,
    sf_api_external_id,
    sf_api_access_token,
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
        instance_url=sf_api_instance_url,
        object=sf_api_object,
        external_id=sf_api_external_id,
    )
    sf_client.login(sf_api_access_token)
    sf_client.set_all_or_none(sf_api_all_or_none)
    sf_client.set_bulk_size(sf_api_bulk_size)
    sf_client.set_dry_run_mode(dry_run)
    if sf_api_req_item_json_template is not None:
        sf_client.set_req_item_json_template(sf_api_req_item_json_template)

    # Fetch data based on input path and send to Salesforce
    dataset = Dataset(
        input,
        file_format=input_file_format,
        csv_file_has_headers=input_csv_file_has_headers,
        logger=logger,
    )
    rows = dataset.get_rows()

    sf_client.send_all_rows(rows, bulk=True)


if __name__ == "__main__":
    main()
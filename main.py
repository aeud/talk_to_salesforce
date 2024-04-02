import csv
import json
import os
import re
import click
import uuid
import requests
import base64

TMP_FILE_PATH = "/tmp/%s.csv" % str(uuid.uuid4())
FILE_FORMAT_CSV = "CSV"

class SaleforceAPIClient():
    """SaleforceBulkAPIClient is a tool we can use to interact with the Salesforce API.
    It contains the credentials, the logic to encode the requests, to interpret the responses.
    """
    def __init__(self, hostname, endpoint, secret_token, method="POST"):
        self.hostname = hostname
        self.endpoint = endpoint
        self.method = method
        self.secret_token = secret_token
        self.bulk_size = 200 # default value
        self.queue = []
        self.queue_size = 0
        self.req_item_json_template = None
        self.dry_run_mode = False
        self.set_session(requests.Session())
        self.auth_session()
        self.reset_queue()
        
    def set_dry_run_mode(self, dry_run_mode):
        self.dry_run_mode = dry_run_mode
    
    def set_all_or_none(self, all_or_none):
        self.all_or_none = all_or_none
    
    def set_bulk_size(self, n):
        self.bulk_size = n

    def set_session(self, custom_session):
        self.session = custom_session
    
    def set_req_item_json_template(self, template):
        from jinja2 import Environment, BaseLoader
        self.req_item_json_template = Environment(loader=BaseLoader()).from_string(template)
    
    def reset_queue(self):
        self.queue = []
        self.queue_size = 0
    
    def custom_json_encoder(self, row):
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
        return self.send_bulk_request(records=[record], all_or_none=all_or_none)
    
    def send_bulk_request(self, records):
        body = json.dumps({
            "allOrNone": self.all_or_none,
            "records": records,
        })
        self.send_http_request(self.method, body)
    
    def queue_item_and_send_bulk_request(self, item):
        self.queue.append(item)
        self.queue_size = self.queue_size + 1
        if self.queue_size >= self.bulk_size:
            self.send_bulk_request(self.queue)
            self.reset_queue()
    
    def send_all_it(self, it, bulk=True):
        for row in it:
            v = self.custom_json_encoder(row)
            if bulk:
                self.queue_item_and_send_bulk_request(v)
            else:
                self.send_single_request(v)
        self.flush()
    
    def flush(self):
        if self.queue_size > 0:
            self.send_bulk_request(self.queue)
    
    def auth_session(self):
        self.session.headers.update({
            "Authorization": "Bearer %s" % self.secret_token,
            "Content-Type": "application/json",
        })
        pass
    
    def send_http_request(self, method="POST", body=None):
        url = "%s%s" % (self.hostname, self.endpoint)
        print("Sending a %s HTTP request to %s" % (
            self.method,
            url,
        ))
        if self.dry_run_mode:
            print("Would have sent", body)
            return
        req = requests.Request(method,  url, data=body)
        prepped = self.session.prepare_request(req)
        if body is not None:
            prepped.body = body
        try:
            resp = self.session.send(prepped)
        except Exception as e:
            print(e)
        if resp.status_code == 200:
            print("request sent", resp.content.decode("utf-8"))
        else:
            print("error when sending the rows", resp.content)
            
        # print(resp.content)

def download_file(project_id, bucket_name, blob_name, tmp_file_path):
    if project_id is None:
        raise Exception("Storage project cannot be null. Add it as an input parameter.")
    from google.cloud.storage import Client
    print("Downloading the file bucket: `%s` and key: `%s` (via project `%s`)..." % (
        bucket_name,
        blob_name,
        project_id,
    ))
    client = Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(tmp_file_path)
    print("Content downloaded in file `%s`" % (
        tmp_file_path,
    ))

def collect_rows(file, input_file_format, input_csv_file_has_headers=False):
    if input_file_format == FILE_FORMAT_CSV:
        if input_csv_file_has_headers:
            reader = csv.DictReader(file)
        else :
            reader = csv.reader(file)
        return reader
    else:
        raise Exception("only CSV file format is developed yet")

def remove_tmp_file(tmp_file_path):
    try:
        os.remove(tmp_file_path)
        print("File `%s` deleted" % (
            tmp_file_path,
        ))
    except OSError:
        pass

def args_secret_wrapper(a):
    if type(a) == str and a.startswith("env://"):
        var_name = a.replace("env://", "")
        val = os.getenv(var_name)
        if val is None:
            raise Exception("environment / secret value %s cannot be null" % var_name)
        return val
    if type(a) == str and re.match(r'^secretmanager\:\/\/projects\/\d+\/secrets\/', a):
        from google.cloud import secretmanager
        client = secretmanager.SecretManagerServiceClient()
        response = client.access_secret_version(request={"name": a.replace("secretmanager://", "")})
        val = response.payload.data.decode("UTF-8")
        if val is None:
            raise Exception("environment / secret value %s cannot be null" % var_name)
        return val
    return a

def storage_parse_path(path):
    if not path.startswith("gs://"):
        raise Exception("Input path must start with gs://")
    elements = path.replace("gs://", "").split("/")
    return elements[0], "/".join(elements[1:])

def bigquery_parse_input(path):
    if not path.startswith("bq://"):
        raise Exception("Input path must start with bq://")
    sql = base64.decodebytes(path.replace("bq://", "").encode("utf-8")).decode("utf-8")
    return sql

def get_rows(project_id, sql):
    if project_id is None:
        raise Exception("BigQuery project cannot be null. Add it as an input parameter.")
    from google.cloud import bigquery
    client = bigquery.Client(project=project_id)
    print("Fetching data from BigQuery (via project `%s`) using the script:\n%s" % (
        project_id,
        sql,
    ))
    query_job = client.query(sql)
    return query_job.result()

@click.command()
@click.option("--input-path", help="TODO", required=True)
@click.option("--input-storage-project-id", help="TODO")
@click.option("--input-bigquery-project-id", help="TODO")
@click.option("--input-csv-file-has-headers", default=False, is_flag=True, show_default=True, help="TODO")
@click.option("--input-file-format", default="CSV", help="TODO")
@click.option("--sf-api-req-item-json-template", help="TODO", default=None)
@click.option("--sf-api-hostname", help="TODO", required=True)
@click.option("--sf-api-endpoint", help="TODO", required=True)
@click.option("--sf-api-credentials", help="TODO", required=True)
@click.option("--sf-api-method", help="TODO", default="POST")
@click.option("--sf-api-bulk-size", default="1000", help="TODO")
@click.option("--sf-api-all-or-none", default=False, is_flag=True, show_default=True, help="TODO")
@click.option("--dry-run", default=False, is_flag=True, show_default=True, help="TODO")
def main(
    input_path,
    input_storage_project_id,
    input_bigquery_project_id,
    input_file_format,
    input_csv_file_has_headers,
    sf_api_req_item_json_template,
    sf_api_hostname,
    sf_api_endpoint,
    sf_api_credentials,
    sf_api_method,
    sf_api_bulk_size,
    sf_api_all_or_none,
    dry_run,
):
    
    # Check secrets or environment variables for relevant variables
    # Variable will use an environment variable when the arg starts with env:// (ex: env://SOME_ENV_VAR_NAME)
    # Variable will use the secret manager when the arg starts with secretmanager:// (ex: secretmanager://SOME_ENV_VAR_NAME)
    sf_api_hostname = args_secret_wrapper(sf_api_hostname)
    sf_api_credentials = args_secret_wrapper(sf_api_credentials)
    sf_api_bulk_size = int(args_secret_wrapper(sf_api_bulk_size))
    
    sf_client = SaleforceAPIClient(sf_api_hostname, sf_api_endpoint, sf_api_credentials, method=sf_api_method)
    sf_client.set_all_or_none(sf_api_all_or_none)
    sf_client.set_bulk_size(sf_api_bulk_size)
    sf_client.set_dry_run_mode(dry_run)
    if sf_api_req_item_json_template is not None:
        sf_client.set_req_item_json_template(sf_api_req_item_json_template)
    
    if input_path.startswith("gs://"):
        input_bucket_name, input_blob_name = storage_parse_path(input_path)
        download_file(input_storage_project_id, input_bucket_name, input_blob_name, TMP_FILE_PATH)
        input_path = "file://%s" % TMP_FILE_PATH
    
    if input_path.startswith("file://"):
        input_file_path = input_path.replace("file://", "")
        file = open(input_file_path)
        it = collect_rows(file, input_file_format, input_csv_file_has_headers=input_csv_file_has_headers)
    elif input_path.startswith("bq://"):
        sql = bigquery_parse_input(input_path)
        it = get_rows(input_bigquery_project_id, sql)
    else:
        raise Exception("Input file path format must be either file:// (file system) or gs:// (Google Cloud Storage)")
    
    sf_client.send_all_it(it, bulk=True)
    
    remove_tmp_file(TMP_FILE_PATH)

if __name__ == "__main__":
    main()
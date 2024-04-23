import csv
import os
import re
import base64
from urllib.parse import urlparse, parse_qs
from functools import reduce


# Constant for defining file format as CSV
FILE_FORMAT_CSV = "CSV"



def download_file(project_id, bucket_name, blob_name, tmp_file_path):
    """Downloads a file from Google Cloud Storage.

    Args:
        project_id (str): Google Cloud Storage project ID.
        bucket_name (str): Name of the bucket containing the file.
        blob_name (str): Name of the file to download.
        tmp_file_path (str): Temporary file path to save the downloaded file.
    """
    from google.cloud.storage import Client
    client = Client(project=project_id)
    bucket = client.bucket(bucket_name)
    blob = bucket.blob(blob_name)
    blob.download_to_filename(tmp_file_path)


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
        # logger.info("File `%s` deleted" % (tmp_file_path,))
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
    elements = path.split("/")
    return elements[0], "/".join(elements[1:])


def bigquery_parse_input(path):
    """Parses a BigQuery input path.

    Args:
        path (str): BigQuery input path containing a base64 encoded SQL query.

    Returns:
        str: Decoded SQL query.
    """
    sql = base64.decodebytes(path.encode("utf-8")).decode("utf-8")
    return sql

def source_parse_input(url):
    v = urlparse(url)
    full_path = v.netloc + v.path
    params = parse_qs(v.query)
    scheme = "file"
    if v.scheme is not None and v.scheme != "":
        scheme = v.scheme
    return scheme, full_path, params

def get_rows(project_id, sql):
    """Fetches rows from BigQuery.

    Args:
        project_id (str): BigQuery project ID.
        sql (str): SQL query to fetch rows.

    Returns:
        iterable: Iterable containing rows fetched from BigQuery.
    """
    from google.cloud import bigquery

    client = bigquery.Client(project=project_id)
    query_job = client.query(sql)
    return query_job.result()

def get_nested_default(d, path):
    return reduce(lambda d, k: d.setdefault(k, {}), path, d)

def set_nested(d, path, value):
    get_nested_default(d, path[:-1])[path[-1]] = value

def unflatten(d, separator='.'):
    output = {}
    for k, v in d.items():
        path = k.split(separator)
        set_nested(output, path, v)
    return output
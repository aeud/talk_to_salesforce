from .utils import download_file, storage_parse_path, collect_rows, bigquery_parse_input, get_rows, source_parse_input
import uuid
import logging

class Dataset:
    def __init__(self, data_source, csv_file_has_headers=False, file_format="CSV", logger=None):
        self.source_method, self.source_full_path, self.source_parameters = source_parse_input(data_source)
        self.csv_file_has_headers = csv_file_has_headers
        self.file_format = file_format
        self.tmp_file_path = "/tmp/%s.csv" % str(uuid.uuid4())
        self.logger = logging.getLogger(__name__)
        self.rows = []
    
    def use_logger(self, logger):
        self.logger = logger

    def fetch_data(self):
        if self.source_method == "gs":
            bucket_name, blob_name = storage_parse_path(self.source_full_path)
            try:
                project_id = self.source_parameters.get("project", [])[0]
            except IndexError:
                project_id = None
            self.logger.info(
                "Downloading the file bucket: `%s` and key: `%s` (via project `%s`)..."
                % (
                    bucket_name,
                    blob_name,
                    project_id if project_id else "default",
                )
            )
            download_file(project_id, bucket_name, blob_name, self.tmp_file_path)
            self.logger.info("Content downloaded in file `%s`" % (self.tmp_file_path,))
            self.source_full_path = self.tmp_file_path
            self.source_method = "file"

        if self.source_method == "file":
            file = open(self.source_full_path)
            self.rows = collect_rows(
                file,
                self.file_format,
                input_csv_file_has_headers=self.csv_file_has_headers,
            )
        elif self.source_method == "bq":
            sql = bigquery_parse_input(self.source_full_path)
            try:
                project_id = self.source_parameters.get("project", [])[0]
            except IndexError:
                project_id = None
            self.logger.info(
                "Fetching data from BigQuery (via project `%s`) using the script:\n%s"
                % (
                    project_id if project_id else "default",
                    sql,
                )
            )
            self.rows = get_rows(project_id, sql)
        else:
            raise Exception(
                "Input file path format must be either file:// (file system) or gs:// (Google Cloud Storage)"
            )

    def get_rows(self):
        if not self.rows:
            self.fetch_data()
        return self.rows
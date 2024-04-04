from .utils import download_file, storage_parse_path, collect_rows, bigquery_parse_input, get_rows
import uuid

class Dataset:
    def __init__(self, data_source, project_id=None, csv_file_has_headers=False, file_format="CSV", logger=None):
        self.data_source = data_source
        self.project_id = project_id
        self.csv_file_has_headers = csv_file_has_headers
        self.file_format = file_format
        self.tmp_file_path = "/tmp/%s.csv" % str(uuid.uuid4())
        self.logger = logger
        self.rows = []

    def fetch_data(self):
        if self.data_source.startswith("gs://"):
            bucket_name, blob_name = storage_parse_path(self.data_source)
            self.logger.info(
                "Downloading the file bucket: `%s` and key: `%s` (via project `%s`)..."
                % (
                    bucket_name,
                    blob_name,
                    self.project_id,
                )
            )
            download_file(self.project_id, bucket_name, blob_name, self.tmp_file_path)
            self.logger.info("Content downloaded in file `%s`" % (self.tmp_file_path,))
            self.data_source = "file://%s" % self.tmp_file_path

        if self.data_source.startswith("file://"):
            input_file_path = self.data_source.replace("file://", "")
            file = open(input_file_path)
            self.rows = collect_rows(
                file,
                self.file_format,
                input_csv_file_has_headers=self.csv_file_has_headers,
            )
        elif self.data_source.startswith("bq://"):
            sql = bigquery_parse_input(self.data_source)
            self.logger.info(
                "Fetching data from BigQuery (via project `%s`) using the script:\n%s"
                % (
                    self.project_id,
                    sql,
                )
            )
            self.rows = get_rows(self.project_id, sql)
        else:
            raise Exception(
                "Input file path format must be either file:// (file system) or gs:// (Google Cloud Storage)"
            )

    def get_rows(self):
        if not self.rows:
            self.fetch_data()
        return self.rows
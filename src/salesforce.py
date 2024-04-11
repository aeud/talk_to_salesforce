import json
import requests

class SaleforceAPIClient:
    """SaleforceBulkAPIClient is a tool we can use to interact with the Salesforce API.
    It contains the credentials, the logic to encode the requests, to interpret the responses.
    """

    def __init__(self, instance_url, endpoint, access_token, logger, method="POST"):
        """Initializes a new instance of the SaleforceAPIClient class.

        Args:
            instance_url (str): URL of the instance that the org lives on.
            endpoint (str): Salesforce API endpoint to send the records to.
            access_token (str): Access token used to authenticate the request.
            logger (logging.Logger): logger used to log.
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
        self.logger = logger
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

    def send_all_rows(self, rows, bulk=True):
        """Sends all items in the iterable 'rows', either individually or in bulk depending on the 'bulk' flag.

        Args:
            it (iterable): Iterable containing items to be sent.
            bulk (bool, optional): Whether to send items in bulk. Defaults to True.
        """
        for row in rows:
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
        self.logger.info(
            "Sending a %s HTTP request to %s"
            % (
                self.method,
                url,
            )
        )
        if self.dry_run_mode:
            self.logger.info("[DRY RUN] Would have sent: %s" % body)
            return
        req = requests.Request(method, url, data=body)
        prepped = self.session.prepare_request(req)
        if body is not None:
            prepped.body = body
        try:
            resp = self.session.send(prepped)
        except Exception as e:
            self.logger.info(e)
        if resp.status_code == 200:
            self.logger.info("request sent %s" % resp.content.decode("utf-8"))
        else:
            self.logger.warning("error when sending the rows", resp.content)
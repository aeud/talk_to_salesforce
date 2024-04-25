import json
import requests
import logging
import re
from .utils import unflatten


def build_api_endpoint(instance_url, version="v58.0", object=None, external_id=None):
    instance_url = re.sub("/$", "", instance_url)
    if object is None:
        raise Exception("Object cannot be none")
    if external_id is not None and external_id != "":
        return "%s/services/data/%s/composite/sobjects/%s/%s" % (
            instance_url,
            version,
            object,
            external_id,
        ), "PATCH"
    return "%s/services/data/%s/composite/sobjects" % (
        instance_url,
        version,
    ), "POST"
class SaleforceAPIClient:
    """SaleforceBulkAPIClient is a tool we can use to interact with the Salesforce API.
    It contains the credentials, the logic to encode the requests, to interpret the responses.
    """

    def __init__(self, instance_url, object, external_id=None):
        """Initializes a new instance of the SaleforceAPIClient class.

        Args:
            TODO
        """
        self.object = object
        self.api_endpoint_url, self.api_http_method = build_api_endpoint(
            instance_url,
            object=object,
            external_id=external_id,
        )
        self.bulk_size = 200  # default value
        self.queue = []
        self.queue_size = 0
        self.req_item_json_template = None
        self.dry_run_mode = False
        self.logger = logging.getLogger(__name__)
        self.set_session(requests.Session())
        self.reset_queue()
        self.access_token = None

    def login(self, access_token):
        """log the client, using the access_token

        Args:
            access_token (string): Oauth2 access token (can be generated using the `sf` command line)
        """
        self.access_token = access_token
        self.auth_session()
    
    def use_logger(self, logger):
        self.logger = logger

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
        else:
            v = dict(row)
        v["attributes"] = {"type": self.object}
        return unflatten(v)

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
        self.send_http_request(self.api_http_method, body)

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
        check_empty_iterator = True
        for row in rows:
            check_empty_iterator = False
            v = self.custom_json_encoder(row)
            if bulk:
                self.queue_item_and_send_bulk_request(v)
            else:
                self.send_single_request(v)
        if check_empty_iterator:
            self.logger.info("No record to send")
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
        url = self.api_endpoint_url
        method = self.api_http_method
        self.logger.info(
            "Sending a %s HTTP request to %s"
            % (
                method,
                url,
            )
        )
        if self.dry_run_mode:
            p = json.loads(body)
            all_or_none = p.get("allOrNone")
            records = p.get("records")
            total = len(records)
            if total > 0:
                self.logger.info("[DRY RUN] Would have sent %d elements (allOrNone: %s)" % (total, all_or_none))
                for i, r in enumerate(records):
                    self.logger.info("[DRY RUN] Record %d/%d: %s" % (i+1, total, r))
            else:
                self.logger.info("[DRY RUN] No record to send")
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
            p = resp.json()
            success_n = 0
            for r in p:
                if r.get("success"):
                    success_n = success_n + 1
                else:
                    self.logger.warning("error when sending %s" % r)
            self.logger.info("request sent (%d/%d)" % (success_n, len(p)))
        else:
            self.logger.warning("error when sending the rows (%s), code: %d", (resp.content, resp.status_code))
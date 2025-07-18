"""Unused for now: Batch streams for complex Mailchimp operations."""

import json
import time
import random
import tarfile
import backoff
import requests
import io
from typing import Iterable, Optional
from singer_sdk import typing as th
from tap_mailchimp.client import ClientRateLimitError, MailchimpStream, Server5xxError
from singer import metrics
import singer

LOGGER = singer.get_logger()

class BatchExpiredError(Exception):
    """Batch expired error."""

class MailchimpClient:
    def __init__(self, config):
        self.__user_agent = config.get('user_agent')
        self.__access_token = config.get('access_token')
        self.__api_key = config.get('api_key')
        self.__session = requests.Session()
        self.__base_url = None
        self.page_size = int(config.get('page_size', '1000'))

        if not self.__access_token and self.__api_key:
            self.__base_url = 'https://{}.api.mailchimp.com'.format(
                config.get('dc'))

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback): # pylint: disable=redefined-builtin
        self.__session.close()

    def get_base_url(self):
        data = self.request('GET',
                            url='https://login.mailchimp.com/oauth2/metadata',
                            endpoint='base_url')
        self.__base_url = data['api_endpoint']

    @backoff.on_exception(backoff.expo,
                          (Server5xxError, ClientRateLimitError, ConnectionError),
                          max_tries=6,
                          factor=3)
    def request(self, method, path=None, url=None, s3=False, **kwargs):
        if url is None and self.__base_url is None:
            self.get_base_url()

        if url is None and path:
            url = self.__base_url + '/3.0' + path

        if 'endpoint' in kwargs:
            endpoint = kwargs['endpoint']
            del kwargs['endpoint']
        else:
            endpoint = None

        if 'headers' not in kwargs:
            kwargs['headers'] = {}

        if not s3:
            if self.__access_token:
                kwargs['headers']['Authorization'] = 'OAuth {}'.format(self.__access_token)
            elif self.__api_key:
                kwargs['auth'] = ('', self.__api_key)
            else:
                raise Exception('`access_token` or `api_key` required')

        if self.__user_agent:
            kwargs['headers']['User-Agent'] = self.__user_agent

        if s3:
            kwargs['stream'] = True

        with metrics.http_request_timer(endpoint) as timer:
            LOGGER.info("Executing %s request to %s with params: %s", method, url, kwargs.get('params'))
            response = self.__session.request(method, url, **kwargs)
            if not s3 and response.json().get("error"):
                LOGGER.error("Error in response: %s", response.json().get("error"))
                raise Exception(response.json().get("error"))
            timer.tags[metrics.Tag.http_status_code] = response.status_code

        if response.status_code >= 500:
            raise Server5xxError()

        if response.status_code == 429:
            raise ClientRateLimitError()

        response.raise_for_status()

        if s3:
            return response

        return response.json()

    def get(self, path, **kwargs):
        return self.request('GET', path=path, **kwargs)

    def post(self, path, **kwargs):
        return self.request('POST', path=path, **kwargs)

class EmailActivityBatchStream(MailchimpStream):
    """Email activity batch stream for handling complex batch operations."""

    name = "reports_email_activity"
    path = "/reports/{campaign_id}/email-activity"
    primary_keys = ["campaign_id", "action", "email_id", "timestamp"]
    replication_key = "timestamp"
    records_jsonpath = "$.emails[*]"
    # parent_stream_type = CampaignsStream

    schema = th.PropertiesList(
        th.Property("campaign_id", th.StringType),
        th.Property("action", th.StringType),
        th.Property("email_id", th.StringType),
        th.Property("timestamp", th.DateTimeType),
        th.Property("url", th.StringType),
        th.Property("ip", th.StringType),
        th.Property("user_agent", th.StringType),
        th.Property("location", th.ObjectType()),
        th.Property("list_id", th.StringType),
        th.Property("list_is_active", th.BooleanType),
        th.Property("email_address", th.StringType),
        th.Property("type", th.StringType),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def __init__(self, tap):
        """Initialize the stream."""
        super().__init__(tap)
        self.batch_size = 100
        self.max_retry_elapsed_time = 43200  # 12 hours
        self.min_retry_interval = 2
        self.max_retry_interval = 300

    def get_campaign_ids(self) -> list:
        """Get campaign IDs from the campaigns stream."""        
        campaign_ids = []
        
        # Get all campaigns that are sent
        params = {
            "status": "sent",
            "sort_field": "send_time",
            "sort_dir": "ASC",
            "count": 100,
        }
        
        response = requests.get(
            f"{self.url_base}/campaigns",
            params=params,
            headers=self.http_headers,
        )
        response.raise_for_status()
        
        data = response.json()
        campaigns = data.get("campaigns", [])
        
        for campaign in campaigns:
            campaign_ids.append(campaign["id"])
        
        return campaign_ids

    def create_batch_operations(self, campaign_ids: list, since_date: str) -> list:
        """Create batch operations for email activity."""
        operations = []
        
        # Get selected fields from schema
        selected_fields = self._get_selected_fields()
        
        for campaign_id in ["000fa74aa0"]:
            since = self.get_starting_replication_key_value({"campaign_id": campaign_id}) or since_date
            operations.append({
                "method": "GET",
                "path": f"/reports/{campaign_id}/email-activity",
                "operation_id": campaign_id,
                "params": {
                    "since": since,
                    "fields": selected_fields
                }
            })
        
        return operations

    def _get_selected_fields(self) -> str:
        """Get selected fields from schema for API request."""
        # For now, return a comprehensive set of fields
        # This can be enhanced later to read from catalog metadata
        # 
        # Future enhancement: Read from self.tap.catalog if available
        # Example:
        # if hasattr(self, 'tap') and self.tap and hasattr(self.tap, 'catalog'):
        #     # Read selected fields from catalog metadata
        #     # This would require proper singer_sdk catalog access
        return "emails.activity,emails.email_id,emails.campaign_id,emails.list_id,emails.list_is_active,emails.email_address,emails.type"

    def create_batch(self, operations: list) -> str:
        """Create a batch job."""
        response = requests.post(
            f"{self.url_base}/batches",
            json={"operations": operations},
            headers=self.http_headers,
        )
        response.raise_for_status()
        data = response.json()
        return data["id"]

    def get_batch_info(self, batch_id: str) -> dict:
        """Get batch job information."""
        try:
            response = requests.get(
                f"{self.url_base}/batches/{batch_id}",
                headers=self.http_headers,
            )
            response.raise_for_status()
            return response.json()
        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                raise BatchExpiredError(f"Batch {batch_id} expired")
            raise e

    def poll_batch(self, batch_id: str) -> dict:
        """Poll batch job until completion."""
        sleep = 0
        start_time = time.time()
        
        while True:
            data = self.get_batch_info(batch_id)
            
            progress = ""
            if data["total_operations"] > 0:
                progress = f" ({data['finished_operations']}/{data['total_operations']} {data['finished_operations'] / data['total_operations'] * 100.0:.2f}%)"
            
            self.logger.info(
                f"reports_email_activity - Job polling: {data['id']} - {data['status']}{progress}"
            )
            
            if data["status"] == "finished":
                return data
            elif (time.time() - start_time) > self.max_retry_elapsed_time:
                message = f"Mailchimp campaigns export is still in progress after {self.max_retry_elapsed_time} seconds. Will continue with this export on the next sync."
                self.logger.error(message)
                raise Exception(message)
            
            sleep = self.next_sleep_interval(sleep)
            self.logger.info(f"campaigns - status: {data['status']}, sleeping for {sleep} seconds")
            time.sleep(sleep)

    def next_sleep_interval(self, previous_sleep_interval: int) -> int:
        """Calculate next sleep interval."""
        min_interval = previous_sleep_interval or self.min_retry_interval
        max_interval = previous_sleep_interval * 2 or self.min_retry_interval
        return min(self.max_retry_interval, random.randint(min_interval, max_interval))

    def stream_activity_from_archive(self, archive_url: str) -> Iterable[dict]:
        """Stream activity records from archive URL."""
        
        def transform_activities(records):
            """Transform activity records to the expected format."""
            for record in records:
                if 'activity' in record:
                    # Remove _links as it's not needed in the output
                    if '_links' in record:
                        del record['_links']
                    
                    # Create base record template
                    record_template = dict(record)
                    del record_template['activity']
                    
                    # Process each activity
                    for activity in record['activity']:
                        new_activity = dict(record_template)
                        # Merge activity fields into the record
                        for key, value in activity.items():
                            new_activity[key] = value
                        yield new_activity

        failed_campaign_ids = []

        with requests.get(archive_url, stream=True) as response:
            with tarfile.open(mode='r|gz', fileobj=response.raw) as tar:
                file = tar.next()
                while file:
                    if file.isfile():
                        rawoperations = tar.extractfile(file)
                        operations = json.loads(rawoperations.read().decode('utf-8'))
                        for i, operation in enumerate(operations):
                            campaign_id = operation['operation_id']
                            last_bookmark = self.get_starting_replication_key_value({"campaign_id": campaign_id})
                            LOGGER.info("reports_email_activity - [batch operation %s] Processing records for campaign %s", i, campaign_id)
                            
                            if operation['status_code'] != 200:
                                failed_campaign_ids.append(campaign_id)
                            else:
                                response = json.loads(operation['response'])
                                email_activities = response['emails']
                                
                                # Process transformed activities
                                for transformed_record in transform_activities(email_activities):
                                    yield transformed_record
                    file = tar.next()

        if failed_campaign_ids:
            self.logger.warning(f"reports_email_activity - operations failed for campaign_ids: {failed_campaign_ids}")

    def sync_batch_email_activity(self, since_date: str, batch_id: Optional[str] = None) -> Iterable[dict]:
        """Sync email activity using batch operations."""
        if batch_id:
            self.logger.info(f"reports_email_activity - Picking up previous run: {batch_id}")
        else:
            self.logger.info("reports_email_activity - Starting sync")
            
            campaign_ids = self.get_campaign_ids()
            if not campaign_ids:
                return
            
            operations = self.create_batch_operations(campaign_ids, since_date)
            batch_id = self.create_batch(operations)
            self.logger.info(f"reports_email_activity - Job running: {batch_id}")

        data = self.poll_batch(batch_id)
        
        from datetime import datetime
        
        completed_at = datetime.fromisoformat(data['completed_at'].replace('Z', '+00:00'))
        submitted_at = datetime.fromisoformat(data['submitted_at'].replace('Z', '+00:00'))
        
        self.logger.info(
            f"reports_email_activity - Batch job complete: took {(completed_at - submitted_at).total_seconds() / 60:.2f} minutes"
        )
        
        yield from self.stream_activity_from_archive(data["response_body_url"])

    def get_records(self, context: Optional[dict]) -> Iterable[dict]:
        """Get records from the stream."""
        since_date = self.config.get("start_date")
        
        # Check for existing batch job
        batch_id = self.get_starting_replication_key_value(context) if context else None
        
        yield from self.sync_batch_email_activity(since_date, batch_id) 

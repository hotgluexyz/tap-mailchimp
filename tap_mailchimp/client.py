"""Mailchimp API client."""

import copy
from typing import Any, Optional
import requests
from singer_sdk.authenticators import BearerTokenAuthenticator, BasicAuthenticator
from singer_sdk.streams import RESTStream
from backports.cached_property import cached_property


class ClientRateLimitError(Exception):
    """Rate limit error."""

class Server5xxError(Exception):
    """Server 5xx error."""

class MailchimpStream(RESTStream):
    """Base class for Mailchimp streams."""

    @cached_property
    def url_base(self) -> str:
        if self.config.get("access_token"):
            # For OAuth, we need to get the base URL from the metadata endpoint
            metadata_response = requests.get(
                "https://login.mailchimp.com/oauth2/metadata",
                headers=self.http_headers
            )
            metadata_response.raise_for_status()
            metadata = metadata_response.json()
            return metadata["api_endpoint"] + '/3.0'

        # For API key, use the data center
        dc = self.config["dc"]
        return f"https://{dc}.api.mailchimp.com/{3.0}"

    def get_authenticator(self):
        """Get the authenticator."""
        if self.config.get("access_token"):
            return BearerTokenAuthenticator.create_for_stream(
                self, token='OAuth {}'.format(self.config.get("access_token"))
            )
        elif self.config.get("api_key"):
            return BasicAuthenticator.create_for_stream(
                self, username="", password=self.config["api_key"]
            )
        else:
            raise Exception("`access_token` or `api_key` required")

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed."""
        headers = {}
        if "user_agent" in self.config:
            headers["User-Agent"] = self.config["user_agent"]
        if self.config.get("access_token"):
            headers["Authorization"] = f"OAuth {self.config.get('access_token')}"
        elif self.config.get("api_key"):
            headers["auth"] = ("", self.config.get("api_key"))
        return headers

    def get_url_params(
        self, context, next_page_token
    ):
        """Get URL parameters."""
        params = {}
        if next_page_token:
            params["offset"] = next_page_token
        return params
    
    def make_request(self, context, next_page_token):
        prepared_request = self.prepare_request(
            context, next_page_token=next_page_token
        )
        resp = self._request(prepared_request, context)
        return resp
    
    def request_records(self, context: Optional[dict]):
        next_page_token: Any = None
        finished = False
        decorated_request = self.request_decorator(self.make_request)

        while not finished:
            resp = decorated_request(context, next_page_token)
            for row in self.parse_response(resp):
                yield row
            previous_token = copy.deepcopy(next_page_token)
            next_page_token = self.get_next_page_token(
                response=resp, previous_token=previous_token
            )
            if next_page_token and next_page_token == previous_token:
                raise RuntimeError(
                    f"Loop detected in pagination. "
                    f"Pagination token {next_page_token} is identical to prior token."
                )
            # Cycle until get_next_page_token() no longer returns a value
            finished = not next_page_token
    
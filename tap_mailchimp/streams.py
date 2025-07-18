"""Mailchimp streams."""
import requests
from typing import Any, Dict, Iterable, Optional
from singer_sdk import typing as th
from tap_mailchimp.client import MailchimpStream

class AutomationsStream(MailchimpStream):
    """Automations stream."""

    name = "automations"
    path = "/automations"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.automations[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("start_time", th.DateTimeType),
        th.Property("status", th.StringType),
        th.Property("emails_sent", th.IntegerType),
        th.Property("recipients", th.IntegerType),
        th.Property("settings", th.ObjectType()),
        th.Property("tracking", th.ObjectType()),
        th.Property("trigger_settings", th.ObjectType()),
        th.Property("report_summary", th.ObjectType()),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        automations = data.get("automations", [])
        if len(automations) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(automations)

class CampaignsStream(MailchimpStream):
    """Campaigns stream."""

    name = "campaigns"
    path = "/campaigns"
    primary_keys = ["id"]
    replication_key = None
    records_jsonpath = "$.campaigns[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("web_id", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("create_time", th.DateTimeType),
        th.Property("send_time", th.DateTimeType),
        th.Property("archive_url", th.StringType),
        th.Property("long_archive_url", th.StringType),
        th.Property("status", th.StringType),
        th.Property("emails_sent", th.IntegerType),
        th.Property("send_time", th.DateTimeType),
        th.Property("content_type", th.StringType),
        th.Property("subject_line", th.StringType),
        th.Property("preview_text", th.StringType),
        th.Property("title", th.StringType),
        th.Property("from_name", th.StringType),
        th.Property("reply_to", th.StringType),
        th.Property("to_name", th.StringType),
        th.Property("folder_id", th.StringType),
        th.Property("template_id", th.IntegerType),
        th.Property("tracking", th.ObjectType()),
        th.Property("delivery_status", th.ObjectType()),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "status": "sent",
            "sort_field": "send_time",
            "sort_dir": "ASC",
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        campaigns = data.get("campaigns", [])
        if len(campaigns) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(campaigns)

    # def parse_response(self, response: requests.Response) -> Iterable[dict]:
    #     """Parse response."""
    #     data = response.json()
    #     campaigns = data.get("campaigns", [])
    #     for campaign in campaigns:
    #         yield campaign
    
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Get child context."""
        return {"campaign_id": record["id"]}

class ListsStream(MailchimpStream):
    """Lists stream."""

    name = "lists"
    path = "/lists"
    primary_keys = ["id"]
    replication_key = None
    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("web_id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("contact", th.ObjectType()),
        th.Property("permission_reminder", th.StringType),
        th.Property("use_archive_bar", th.BooleanType),
        th.Property("campaign_defaults", th.ObjectType()),
        th.Property("notify_on_subscribe", th.StringType),
        th.Property("notify_on_unsubscribe", th.StringType),
        th.Property("date_created", th.DateTimeType),
        th.Property("list_rating", th.IntegerType),
        th.Property("email_type_option", th.BooleanType),
        th.Property("subscribe_url_short", th.StringType),
        th.Property("subscribe_url_long", th.StringType),
        th.Property("beamer_address", th.StringType),
        th.Property("visibility", th.StringType),
        th.Property("double_optin", th.BooleanType),
        th.Property("has_welcome", th.BooleanType),
        th.Property("welcome_id", th.StringType),
        th.Property("stats", th.ObjectType()),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "sort_field": "date_created",
            "sort_dir": "ASC",
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    # def get_next_page_token(
    #     self, response: requests.Response, previous_token: Optional[Any]
    # ) -> Optional[Any]:
    #     """Get next page token."""
    #     data = response.json()
    #     lists = data.get("lists", [])
    #     if len(lists) < self.config.get("page_size", 1000):
    #         return None
    #     return (previous_token or 0) + len(lists)

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse response."""
        data = response.json()
        lists = data.get("lists", [])
        for list_item in lists:
            yield list_item

    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Get child context."""
        return {"list_id": record["id"]}

class ListMembersStream(MailchimpStream):
    """List members stream."""

    name = "list_members"
    path = "/lists/{list_id}/members"
    primary_keys = ["id", "list_id"]
    replication_key = "last_changed"
    records_jsonpath = "$.members[*]"
    parent_stream_type = ListsStream

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("email_address", th.StringType),
        th.Property("unique_email_id", th.StringType),
        th.Property("contact_id", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("web_id", th.IntegerType),
        th.Property("email_type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("unsubscribe_reason", th.StringType),
        th.Property("consents_to_one_to_one_messaging", th.BooleanType),
        th.Property("merge_fields", th.ObjectType()),
        th.Property("interests", th.ObjectType()),
        th.Property("stats", th.ObjectType()),
        th.Property("ip_signup", th.StringType),
        th.Property("timestamp_signup", th.DateTimeType),
        th.Property("ip_opt", th.StringType),
        th.Property("timestamp_opt", th.DateTimeType),
        th.Property("member_rating", th.IntegerType),
        th.Property("last_changed", th.DateTimeType),
        th.Property("language", th.StringType),
        th.Property("vip", th.BooleanType),
        th.Property("email_client", th.StringType),
        th.Property("location", th.ObjectType()),
        th.Property("marketing_permissions", th.ArrayType(th.ObjectType())),
        th.Property("last_note", th.ObjectType()),
        th.Property("source", th.StringType),
        th.Property("tags_count", th.IntegerType),
        th.Property("tags", th.ArrayType(th.ObjectType())),
        th.Property("list_id", th.StringType),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        
        # Add since_last_changed for incremental sync
        if self.get_starting_replication_key_value(context):
            params["since_last_changed"] = self.get_starting_replication_key_value(context)
        
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        members = data.get("members", [])
        if len(members) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(members)

    # def transform_record(self, record: dict) -> dict:
    #     """Transform record."""
    #     # Add list_id to record
    #     if "list_id" not in record:
    #         record["list_id"] = self.context.get("list_id")
        
    #     # Flatten merge_fields
    #     merge_fields = record.get("merge_fields")
    #     if merge_fields:
    #         for key, value in merge_fields.items():
    #             record[key] = value
    #         record.pop("merge_fields", None)
        
    #     return record


class ListSegmentsStream(MailchimpStream):
    """List segments stream."""

    name = "list_segments"
    path = "/lists/{list_id}/segments"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListsStream
    records_jsonpath = "$.segments[*]"


    schema = th.PropertiesList(
        th.Property("id", th.IntegerType),
        th.Property("name", th.StringType),
        th.Property("member_count", th.IntegerType),
        th.Property("type", th.StringType),
        th.Property("created_at", th.DateTimeType),
        th.Property("updated_at", th.DateTimeType),
        th.Property("options", th.ObjectType()),
        th.Property("list_id", th.StringType),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        segments = data.get("segments", [])
        if len(segments) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(segments)

    # def transform_record(self, record: dict) -> dict:
    #     """Transform record."""
    #     # Add list_id to record
    #     if "list_id" not in record:
    #         record["list_id"] = self.context.get("list_id")
    #     return record

    def parse_response(self, response: requests.Response) -> Iterable[dict]:
        """Parse response."""
        data = response.json()
        segments = data.get("segments", [])
        for segment in segments:
            yield segment
            
    def get_child_context(self, record: dict, context: Optional[dict]) -> dict:
        """Get child context."""
        return {"list_id": record["list_id"], "segment_id": record["id"]}


class ListSegmentMembersStream(MailchimpStream):
    """List segment members stream."""

    name = "list_segment_members"
    path = "/lists/{list_id}/segments/{segment_id}/members"
    primary_keys = ["id"]
    replication_key = None
    parent_stream_type = ListSegmentsStream
    records_jsonpath = "$.members[*]"

    schema = th.PropertiesList(
        th.Property("id", th.StringType),
        th.Property("email_address", th.StringType),
        th.Property("unique_email_id", th.StringType),
        th.Property("contact_id", th.StringType),
        th.Property("full_name", th.StringType),
        th.Property("web_id", th.IntegerType),
        th.Property("email_type", th.StringType),
        th.Property("status", th.StringType),
        th.Property("unsubscribe_reason", th.StringType),
        th.Property("consents_to_one_to_one_messaging", th.BooleanType),
        th.Property("merge_fields", th.ObjectType()),
        th.Property("interests", th.ObjectType()),
        th.Property("stats", th.ObjectType()),
        th.Property("ip_signup", th.StringType),
        th.Property("timestamp_signup", th.DateTimeType),
        th.Property("ip_opt", th.StringType),
        th.Property("timestamp_opt", th.DateTimeType),
        th.Property("member_rating", th.IntegerType),
        th.Property("last_changed", th.DateTimeType),
        th.Property("language", th.StringType),
        th.Property("vip", th.BooleanType),
        th.Property("email_client", th.StringType),
        th.Property("location", th.ObjectType()),
        th.Property("marketing_permissions", th.ArrayType(th.ObjectType())),
        th.Property("last_note", th.ObjectType()),
        th.Property("source", th.StringType),
        th.Property("tags_count", th.IntegerType),
        th.Property("tags", th.ArrayType(th.ObjectType())),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        members = data.get("members", [])
        if len(members) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(members)

    # def transform_record(self, record: dict) -> dict:
    #     """Transform record."""
    #     # Flatten merge_fields
    #     merge_fields = record.get("merge_fields")
    #     if merge_fields:
    #         for key, value in merge_fields.items():
    #             record[key] = value
    #         record.pop("merge_fields", None)
    #     return record


class UnsubscribesStream(MailchimpStream):
    """Unsubscribes stream."""

    name = "unsubscribes"
    path = "/reports/{campaign_id}/unsubscribed"
    primary_keys = ["campaign_id", "email_id"]
    replication_key = None
    parent_stream_type = CampaignsStream
    records_jsonpath = "$.unsubscribes[*]"

    schema = th.PropertiesList(
        th.Property("campaign_id", th.StringType),
        th.Property("email_id", th.StringType),
        th.Property("email_address", th.StringType),
        th.Property("list_id", th.StringType),
        th.Property("list_is_active", th.BooleanType),
        th.Property("contact_status", th.StringType),
        th.Property("merge_fields", th.ObjectType()),
        th.Property("vip", th.BooleanType),
        th.Property("location", th.ObjectType()),
        th.Property("timestamp", th.DateTimeType),
        th.Property("reason", th.StringType),
        th.Property("_links", th.ArrayType(th.ObjectType())),
    ).to_dict()

    def get_url_params(
        self, context: Optional[dict], next_page_token: Optional[Any]
    ) -> Dict[str, Any]:
        """Get URL parameters."""
        params = {
            "count": self.config.get("page_size", 1000),
        }
        if next_page_token:
            params["offset"] = next_page_token
        return params

    def get_next_page_token(
        self, response: requests.Response, previous_token: Optional[Any]
    ) -> Optional[Any]:
        """Get next page token."""
        data = response.json()
        unsubscribes = data.get("unsubscribes", [])
        if len(unsubscribes) < self.config.get("page_size", 1000):
            return None
        return (previous_token or 0) + len(unsubscribes)

    # def transform_record(self, record: dict) -> dict:
    #     """Transform record."""
    #     # Add campaign_id to record
    #     if "campaign_id" not in record:
    #         record["campaign_id"] = self.context.get("campaign_id")
        
    #     # Flatten merge_fields
    #     merge_fields = record.get("merge_fields")
    #     if merge_fields:
    #         for key, value in merge_fields.items():
    #             record[key] = value
    #         record.pop("merge_fields", None)
        
    #     return record

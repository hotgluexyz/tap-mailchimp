"""Mailchimp tap class."""

from typing import List

from singer_sdk import Tap, Stream
from singer_sdk.typing import (
    ArrayType,
    BooleanType,
    DateTimeType,
    IntegerType,
    NumberType,
    ObjectType,
    PropertiesList,
    Property,
    StringType,
)

from tap_mailchimp_v2.streams import (
    AutomationsStream,
    CampaignsStream,
    ListMembersStream,
    ListSegmentMembersStream,
    ListSegmentsStream,
    ListsStream,
    UnsubscribesStream,
)
from tap_mailchimp_v2.batch_streams import EmailActivityBatchStream

STREAM_TYPES = [
  AutomationsStream,
  CampaignsStream,
  ListMembersStream,
  ListSegmentMembersStream,
  ListSegmentsStream,
  ListsStream,
  UnsubscribesStream,
  EmailActivityBatchStream
]

class TapMailchimp(Tap):
    """Mailchimp tap class."""

    name = "tap-mailchimp"

    config_jsonschema = PropertiesList(
        Property("access_token", StringType, description="OAuth access token"),
        Property("api_key", StringType, description="API key"),
        Property("dc", StringType, description="Data center (e.g., us1)"),
        Property("start_date", StringType, description="Start date for incremental syncs"),
        Property("page_size", IntegerType, default=1000, description="Page size for API requests"),
        Property("user_agent", StringType, description="User agent for API requests"),
    ).to_dict()

    def discover_streams(self) -> List[Stream]:
        """Return a list of discovered streams."""
        return [stream_class(tap=self) for stream_class in STREAM_TYPES]

def main():
    """Main entry point."""
    TapMailchimp.cli()


if __name__ == "__main__":
    main() 

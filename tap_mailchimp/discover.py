from singer.catalog import Catalog, CatalogEntry, Schema

from tap_mailchimp.schema import get_schemas, PKS


def get_type(field_type):
    if field_type == "number":
        return {"type": ["null", "number"]}
    elif field_type == "date":
        return {"type": ["null", "string"], "format": ["date-time"]}
    elif field_type == "address":
        return {
            "type": ["null", "object"],
            "properties": {
                "addr1": {"type": ["null", "string"]},
                "addr2": {"type": ["null", "string"]},
                "city": {"type": ["null", "string"]},
                "state": {"type": ["null", "string"]},
                "zip": {"type": ["null", "string"]},
                "country": {"type": ["null", "string"]},
            },
        }
    else:
        return {"type": ["null", "string"]}


def discover(client):
    schemas, field_metadata = get_schemas()
    catalog = Catalog([])

    # fetch merge fields for lists endpoints
    merge_fields = {}
    lists = client.get("/lists", params={"count": 1000}).get("lists")
    for list in lists:
        list_merge_fields = client.get(
            f'/lists/{list["id"]}/merge-fields', params={"count": 1000}
        ).get("merge_fields")
        if list_merge_fields:
            list_merge_fields = {
                merge_field["tag"]: merge_field["type"]
                for merge_field in list_merge_fields
            }
            merge_fields.update(list_merge_fields)

    for stream_name, schema_dict in schemas.items():
        # add merge fields for lists streams
        if stream_name in ["list_members", "list_segment_members", "unsubscribes"]:
            for merge_field in merge_fields:
                schema_dict["properties"].update(
                    {merge_field: get_type(merge_fields[merge_field])}
                )
                field_metadata[stream_name].append(
                    {
                        "metadata": {"inclusion": "available"},
                        "breadcrumb": ["properties", merge_field],
                    }
                )

        schema = Schema.from_dict(schema_dict)
        metadata = field_metadata[stream_name]
        pk = PKS[stream_name]

        catalog.streams.append(
            CatalogEntry(
                stream=stream_name,
                tap_stream_id=stream_name,
                key_properties=pk,
                schema=schema,
                metadata=metadata,
            )
        )

    return catalog

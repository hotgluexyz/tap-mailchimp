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
    get_merge_fields(client, merge_fields, lists)

    for stream_name, schema_dict in schemas.items():
        # add merge fields for lists streams
        schema_dict["properties"].pop("merge_fields", None)
        if stream_name in ["list_segments", "list_members", "list_segment_members"]:
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

def get_merge_fields(client, merge_fields, lists, value_as_type=True):
    for mailchimp_list in lists:
        list_merge_fields = client.get(
            f'/lists/{mailchimp_list["id"]}/merge-fields', params={"count": 1000}
        ).get("merge_fields")
        if list_merge_fields:
            [list_merge_field.pop("_links", None) for list_merge_field in list_merge_fields]
            merge_fields.update({
                merge_field["tag"]: "string" if value_as_type else extract_merge_fields(merge_field)
                for merge_field in list_merge_fields
            })
            
def extract_merge_fields(merge_field):
    extracted_merge_field = {
        "tag": merge_field["tag"],
        "value": flatten_dict(merge_field)
    }

    return extracted_merge_field

def flatten_dict(d, parent_key='', sep='.'):
    """
    Flattens a nested dictionary, concatenating keys with a separator.
    Handles lists by including the index in the key.
    """
    def flatten_item(parent_key, key, value, sep):
        new_key = f"{parent_key}{sep}{key}" if parent_key else key
        if isinstance(value, dict):
            return flatten_dict(value, new_key, sep=sep).items()
        elif isinstance(value, list):
            items = []
            for i, item in enumerate(value):
                list_key = f"{new_key}{sep}{i}"
                if isinstance(item, (dict, list)):
                    items.extend(flatten_item(new_key, str(i), item, sep))
                else:
                    items.append((list_key, item))
            return items
        else:
            return [(new_key, value)]

    items = []
    for k, v in d.items():
        items.extend(flatten_item(parent_key, k, v, sep))
    return dict(items)
from tap_kit.streams import Stream
import singer

LOGGER = singer.get_logger()


class EntriesStream(Stream):
    stream = 'entries'

    meta_fields = dict(
        key_properties=['sys.id'],
        api_path='entries',
        replication_method='incremental',
        replication_key='last_updated',
        incremental_search_key='sys.updatedAt',
        selected_by_default=False
    )

    schema = {
        "properties": {
            "sys": {
                "type": ["null", "object"]
            },
            "fields": {
                "type": ["null", "object"]
            },
        }
    }

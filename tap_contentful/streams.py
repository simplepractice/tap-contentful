from tap_kit.streams import Stream
import singer

LOGGER = singer.get_logger()


class EntriesStream(Stream):
    """
    This has the potential to be an incremental stream; however, due to the small
    volume of data we've seen from this integration, it is more efficient to query the
    data if we fully extract the stream. This is because there will be one output file
    for Athena to query, rather than a bunch of tiny files.
    """
    stream = 'entries'

    meta_fields = dict(
        key_properties=['sys.id'],
        api_path='entries',
        replication_method='full',
        # replication_key='last_updated',
        # incremental_search_key='sys.updatedAt',
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

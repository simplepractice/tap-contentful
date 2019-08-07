from tap_kit.streams import Stream
from tap_kit.utils import safe_to_iso8601
import singer

LOGGER = singer.get_logger()


class ContentfulStream(Stream):
    """
    methods to track state for each individual ABC Financial club
    """

    def __init__(self, config=None, state=None, catalog=None):
        super(ContentfulStream, self).__init__(config, state, catalog)

        self.config = config
        self.state = state
        self.catalog = catalog
        self.api_path = self.api_path if self.api_path else self.stream

        self.build_params()

    def write_bookmark(self, state, tap_stream_id, resource_id, key, val):
        state = self.ensure_bookmark_path(state, ['bookmarks',
                                                  tap_stream_id,
                                                  resource_id])
        state['bookmarks'][tap_stream_id][resource_id][key] = val
        return state

    @staticmethod
    def ensure_bookmark_path(state, path):
        """
        :param state: state object
        :param path: array of keys to check in state
        :return: checks for or creates a nested object in which each element
        of the path array is the parent key of the next element
        """
        submap = state
        for path_component in path:
            if submap.get(path_component) is None:
                submap[path_component] = {}

            submap = submap[path_component]
        return state

    def get_bookmark(self, resource_id):
        key = self.stream_metadata.get('replication-key')

        return self.state.get('bookmarks', {})\
                         .get(self.stream, {})\
                         .get(resource_id, {})\
                         .get(key)

    def update_bookmark(self, last_updated, resource_id):
        self.write_bookmark(self.state,
                            self.stream,
                            resource_id,
                            self.stream_metadata.get('replication-key'),
                            safe_to_iso8601(last_updated))
        singer.write_state(self.state)

    def update_start_date_bookmark(self, resource_id):
        val = self.get_bookmark(resource_id)
        if not val:
            val = self.config['start_date']
            self.update_bookmark(val, resource_id)

    def update_and_return_bookmark(self, resource_id):
        self.update_start_date_bookmark(resource_id)
        return self.get_bookmark(resource_id)


class SpacesStream(ContentfulStream):
    stream = 'spaces'

    meta_fields = dict(
        key_properties=['sys.id'],
        api_path='spaces',
        replication_method='incremental',
        replication_key='last_updated',
        incremental_search_key='sys.updatedAt',
        selected_by_default=False
    )

    schema = {
        "properties": {
            "sys": {
                "type": ["null", "string"]
            },
            "fields": {
                "type": ["null", "string"]
            },
        }
    }

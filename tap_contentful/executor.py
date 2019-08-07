import singer

from tap_kit import TapExecutor
from tap_kit.utils import (transform_write_and_count,
                           format_last_updated_for_request)
from .streams import ContentfulStream

LOGGER = singer.get_logger()


class ContentfulExecutor(TapExecutor):

    def __init__(self, streams, args, client):
        """
        Args:
            streams (arr[Stream])
            args (dict)
            client (BaseClient)
        """
        super(ContentfulExecutor, self).__init__(streams, args, client)

        self.replication_key_format = 'datetime_string'
        self.base_url = 'https://cdn.contentful.com'
        self.access_token = self.client.config['access_token']

    def sync(self):
        self.set_catalog()

        for c in self.selected_catalog:
            self.sync_stream(
                ContentfulStream(config=self.config, state=self.state, catalog=c)
            )

    def sync_stream(self, stream):
        stream.write_schema()

        if stream.is_incremental:
            stream.set_stream_state(self.state)
            self.call_incremental_stream(stream)
        else:
            self.call_full_stream(stream)

    def call_incremental_stream(self, stream):
        """
        Method to call all incremental synced streams
        """

        # need to call each resource ID individually
        for space_id in self.client.config['space_ids']:

            last_updated = format_last_updated_for_request(
                stream.update_and_return_bookmark(space_id),
                self.replication_key_format
            )

            request_config = {
                'url': self.generate_api_url(stream, space_id),
                'params': self.build_initial_params(last_updated),
                'run': True
            }

            LOGGER.info("Extracting space {s} since {d}".format(s=space_id,
                                                                d=last_updated))

            while request_config['run']:
                res = self.client.make_request(request_config)

                records = res.json().get('items')

                LOGGER.info('Received {n} records for space {s}'.format(n=len(records),
                                                                        s=space_id))

                transform_write_and_count(stream, records)

                last_updated = self.get_latest_record_date(records)

                LOGGER.info('Setting last updated for space {s} to {d}'.format(
                    s=space_id,
                    d=last_updated
                ))
                stream.update_bookmark(last_updated, space_id)

                request_config = self.update_for_next_call(len(records), request_config)

            return last_updated

    def call_full_stream(self, stream):
        """
        Method to call all fully synced streams
        """
        for space_id in self.client.config['space_ids']:
            request_config = {
                'url': self.generate_api_url(stream, space_id),
                'params': self.build_params(stream),
                'run': True
            }

            LOGGER.info("Extracting data for space {s}".format(s=space_id))

            while request_config['run']:
                res = self.client.make_request(request_config)

                records = res.json().get('items')

                LOGGER.info('Received {n} records for space {s}'.format(n=len(records),
                                                                        s=space_id))

                transform_write_and_count(stream, records)

                request_config = self.update_for_next_call(len(records), request_config)

    def generate_api_url(self, stream, space_id):
        return '/'.join([self.base_url, stream.stream_metadata['api-path'], space_id,
                         'environments/master/entries'])

    @staticmethod
    def get_latest_record_date(records):
        """
        Returns the date from the most recent record received

        ASSUMPTIONS:
        - The last record in the array will always be the most recent record. We do
          this by ordering the records by datetime at the time of the API call (see
          `self.build_initial_params`)
        """
        last_record = records[-1]
        return last_record['sys']['updatedAt']

    def build_initial_params(self, last_updated):
        return {
            'access_token': self.access_token,
            'limit': 1000,
            'skip': 0,
            'order': 'sys.updatedAt',
            'sys.updatedAt[gt]': last_updated
        }

    @staticmethod
    def build_next_params(params):
        if params.get('skip') is not None:
            params['skip'] += 1000
        return params

    def update_for_next_call(self, num_records_received, request_config):
        if num_records_received < 1000:  # 1000 is the max num of records per request
            return {
                "url": request_config['url'],
                "params": request_config['params'],
                "run": False
            }
        else:
            return {
                "url": request_config['url'],
                "params": self.build_next_params(request_config['params']),
                "run": True
            }

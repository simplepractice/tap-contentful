import singer

from tap_kit import TapExecutor
from tap_kit.utils import (transform_write_and_count,
                           format_last_updated_for_request)


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
        self.space_id = self.client.config['space_id']

    def call_full_stream(self, stream):
        """
        Method to call all fully synced streams
        """

        request_config = {
            'url': self.generate_api_url(stream),
            'headers': self.build_headers(),
            'params': self.build_initial_params(),
            'run': True
        }

        LOGGER.info("Extracting {}".format(stream))

        while request_config['run']:
            res = self.client.make_request(request_config)

            records = res.json().get('items')

            LOGGER.info('Received {n} records'.format(n=len(records)))

            transform_write_and_count(stream, records)

            request_config = self.update_for_next_call(len(records), request_config)

    def call_incremental_stream(self, stream):
        """
        Method to call all incremental synced streams
        """
        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(),
            self.replication_key_format
        )

        request_config = {
            'url': self.generate_api_url(stream),
            "headers": self.build_headers(),
            'params': self.build_initial_params(last_updated),
            'run': True
        }

        LOGGER.info("Extracting stream {s} since {d}".format(s=stream,
                                                             d=last_updated))

        while request_config['run']:
            res = self.client.make_request(request_config)

            records = res.json().get('items')

            LOGGER.info('Received {n} records'.format(n=len(records)))

            transform_write_and_count(stream, records)

            last_updated = self.get_latest_record_date(records)

            LOGGER.info('Setting last updated for stream {s} to {d}'.format(
                s=stream,
                d=last_updated
            ))
            stream.update_bookmark(last_updated)

            request_config = self.update_for_next_call(len(records), request_config)

        return last_updated

    def generate_api_url(self, stream):
        return '/'.join([self.base_url, 'spaces', self.space_id, 'environments/master',
                         stream.stream_metadata['api-path']])

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

    def build_initial_params(self, last_updated=None):
        base_params = {
            'access_token': self.access_token,
            'limit': 100,
            'skip': 0,
            'order': 'sys.updatedAt',
        }

        if last_updated:
            # for extracting incrementally
            base_params['sys.updatedAt[gt]'] = last_updated

        return base_params

    @staticmethod
    def build_next_params(params):
        if params.get('skip') is not None:
            params['skip'] += 10
        return params

    def update_for_next_call(self, num_records_received, request_config):
        if num_records_received < 10:  # 10 is the max num of records per request
            return {
                "url": request_config['url'],
                "headers": {},
                "params": request_config['params'],
                "run": False
            }
        else:
            return {
                "url": request_config['url'],
                "headers": {},
                "params": self.build_next_params(request_config['params']),
                "run": True
            }

    @staticmethod
    def build_headers():
        return {}

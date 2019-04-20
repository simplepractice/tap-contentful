import singer
import pendulum

from tap_kit import TapExecutor
from tap_kit.utils import timestamp_to_iso8601
from tap_kit.utils import (transform_write_and_count,
                           format_last_updated_for_request)
from .streams import ABCStream

LOGGER = singer.get_logger()


class ABCExecutor(TapExecutor):

    def __init__(self, streams, args, client):
        """
        Args:
            streams (arr[Stream])
            args (dict)
            client (BaseClient)
        """
        super(ABCExecutor, self).__init__(streams, args, client)

        self.replication_key_format = 'datetime_string'
        self.url = 'https://api.abcfinancial.com/rest/'
        self.api_key = self.client.config['api_key']
        self.app_id = self.client.config['app_id']

    def sync(self):
        self.set_catalog()

        for c in self.selected_catalog:
            self.sync_stream(
                ABCStream(config=self.config, state=self.state, catalog=c)
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

        # need to call each club ID individually
        for club_id in self.client.config['club_ids']:

            last_updated, new_bookmark = self.get_time_range(stream, club_id)

            request_config = {
                'url': self.generate_api_url(stream, club_id),
                'headers': self.build_headers(),
                'params': self.build_initial_params(stream, last_updated, new_bookmark),
                'run': True
            }

            LOGGER.info("Extracting {s} for club {c} since {d}".format(s=stream, 
                                                                       c=club_id, 
                                                                       d=last_updated))

            self.call_stream(stream, club_id, request_config)

            LOGGER.info('Setting last updated for club {} to {}'.format(
                club_id,
                new_bookmark
            ))

            stream.update_bookmark(new_bookmark, club_id)

    def call_full_stream(self, stream):
        """
        Method to call all fully synced streams
        """
        for club_id in self.client.config['club_ids']:
            request_config = {
                'url': self.generate_api_url(stream, club_id),
                'headers': self.build_headers(),
                'params': self.build_params(stream),
                'run': True
            }

            LOGGER.info("Extracting {s} for club {c}".format(s=stream, 
                                                             c=club_id))

            self.call_stream(stream, club_id, request_config)

    def call_stream(self, stream, club_id, request_config):
        while request_config['run']:
            res = self.client.make_request(request_config)

            if stream.is_incremental:
                LOGGER.info('Received {n} records on page {i} for club {c}'.format(
                    n=res.json()['status']['count'],
                    i=res.json()['request']['page'],
                    c=club_id
                ))
            else:
                LOGGER.info('Received {n} records for club {c}'.format(
                    n=res.json()['status']['count'],
                    c=club_id
                ))

            records = res.json().get(stream.stream_metadata['response-key'])

            if not isinstance(records, list):
                # subsequent methods are expecting a list
                records = [records]

            # for endpoints that do not provide club_id
            if stream.stream in streams_to_hydrate:
                records = self.hydrate_record_with_club_id(records, club_id)

            transform_write_and_count(stream, records)

            request_config = self.update_for_next_call(
                res,
                request_config,
                stream
            )

        return request_config

    def generate_api_url(self, stream, club_id):
        return self.url + club_id + stream.stream_metadata['api-path']

    def build_headers(self):
        """
        Included in all API calls
        """
        return {
            "Accept": "application/json;charset=UTF-8",  # necessary for returning JSON
            "app_id": self.app_id,
            "app_key": self.api_key,
        }

    def get_time_range(self, stream, club_id):
        last_updated = format_last_updated_for_request(
            stream.update_and_return_bookmark(club_id),
            self.replication_key_format
        )

        # the checkins endpoint only extracts in 31 day windows, so
        # `new_bookmark` needs to account for that
        if stream.stream == 'checkins' and \
                last_updated == '1970-01-01 00:00:00':
            last_updated = pendulum.datetime(2015, 1, 1)
            new_bookmark = last_updated.add(days=31)

        elif stream.stream == 'checkins':
            dt = pendulum.parse(last_updated)
            new_bookmark = dt.add(days=31)

        else:
            new_bookmark = str(pendulum.now('UTC'))

        return str(last_updated), str(new_bookmark)

    def format_last_updated(self, last_updated):
        """
        Args:
            last_updated(str): datetime string in ISO 8601 format
        Return:
            datetime string in the following format: 'YYYY-MM-DD hh:mm:ss.nnnnnn'
            (necessary format for ABC Financial API)
        """
        datetime = pendulum.parse(last_updated).to_datetime_string() + '.000000'
        return datetime

    def build_initial_params(self, stream, last_updated, new_bookmark):
        date_range = '{p},{c}'.format(p=self.format_last_updated(last_updated),
                                      c=self.format_last_updated(new_bookmark))
        return {
            stream.stream_metadata[stream.filter_key]: date_range,
            'page': 1
        }

    def update_for_next_call(self, res, request_config, stream):
        if int(res.json()['status']['count']) < 5000:
            return {
                "url": self.url,
                "headers": request_config['headers'],
                "params": request_config['params'],
                "run": False
            }
        else:
            return {
                "url": request_config['url'],
                "headers": request_config['headers'],
                "params": self.build_next_params(request_config['params']),
                "run": True
            }

    def build_next_params(self, params):
        if params.get('page'):
            params['page'] += 1
        return params

    def hydrate_record_with_club_id(self, records, club_id):
        """
        Args:
            records (array [JSON]):
            club_id (str):
        Returns:
            array of records, with the club_id appended to each record
        """
        for record in records:
            record['club_id'] = club_id

        return records


streams_to_hydrate = ['prospects', 'clubs']

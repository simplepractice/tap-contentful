import singer

from .client import BaseClient
from .streams import EntriesStream
from .executor import ContentfulExecutor
from .utils import main_method


LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
	"start_date",
	"access_token",
	"space_id",
]

STREAMS = [
	EntriesStream,
]


def main():
	main_method(
		REQUIRED_CONFIG_KEYS,
		ContentfulExecutor,
		BaseClient,
		STREAMS
	)


if __name__ == '__main__':
	main()

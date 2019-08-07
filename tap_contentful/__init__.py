from tap_kit import main_method, BaseClient
from .streams import SpacesStream
from .executor import ContentfulExecutor


REQUIRED_CONFIG_KEYS = [
	"start_date",
	"access_token",
]

STREAMS = [
	SpacesStream,
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

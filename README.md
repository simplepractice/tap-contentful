# tap-contenful

Singer tap to extract data from the Contentful Content Delivery API, conforming to the Singer
spec: https://github.com/singer-io/getting-started/blob/master/docs/SPEC.md

Content Delivery API: https://www.contentful.com/developers/docs/references/content-delivery-api/

## Setup

`python3 setup.py install`

## Running the tap

#### Discover mode:

`tap-contentful --config tap_config.json --discover > catalog.json`

#### Sync mode:

`tap-contentful --config tap_config.json -p catalog.json -s state.json`
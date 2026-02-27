#!/usr/bin/env python3

import sys
import json
import argparse

import singer
from singer import metadata

from tap_mailchimp.client import MailchimpClient
from tap_mailchimp.discover import discover
from tap_mailchimp.sync import sync, sync_parallel

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = [
    'start_date'
]

def do_discover(client):
    LOGGER.info('Testing authentication')
    try:
        client.get('/lists', params={'count': 1})
    except:
        raise Exception('Error testing Mailchimp authentication')

    LOGGER.info('Starting discover')
    catalog = discover(client)
    json.dump(catalog.to_dict(), sys.stdout, indent=2)
    LOGGER.info('Finished discover')

@singer.utils.handle_top_exception(LOGGER)
def main():
    parsed_args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)

    with MailchimpClient(parsed_args.config) as client:
        if parsed_args.discover:
            do_discover(client)
        elif parsed_args.catalog:
            # Use parallel sync if enabled in config
            use_parallel = parsed_args.config.get('use_parallel', False)
            if use_parallel:
                LOGGER.info('Using parallel sync mode')
                sync_parallel(client,
                             parsed_args.catalog,
                             parsed_args.state,
                             parsed_args.config['start_date'])
            else:
                sync(client,
                     parsed_args.catalog,
                     parsed_args.state,
                     parsed_args.config['start_date'])

if __name__ == '__main__':
    main()

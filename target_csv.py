#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import csv
import threading
import http.client
import urllib
from datetime import datetime
import collections
import pkg_resources

from jsonschema.validators import Draft4Validator
import singer

logger = singer.get_logger()

def emit_state(state):
    if state is not None:
        pass
        # line = json.dumps(state)
        # logger.debug('Emitting state {}'.format(line))
        # sys.stdout.write("{}\n".format(line))
        # sys.stdout.flush()

def extract_header_names(property=None, parent_key='', sep='__', selected=False):
    items = []
    for index, key in enumerate(property):    
        value = property[key]
        if selected or value.get('selected', True):
            new_key = parent_key + sep + key if parent_key else key    
            if 'properties' in value:
                items.extend(extract_header_names(property=value['properties'], parent_key=new_key, sep=sep, selected=True))
            else:        
                items.append(new_key)
    return items

def flatten(d, parent_key='', sep='__'):
    items = []
    for k, v in d.items():
        new_key = parent_key + sep + k if parent_key else k
        if isinstance(v, collections.MutableMapping):
            items.extend(flatten(v, new_key, sep=sep).items())
        else:
            items.append((new_key, str(v) if type(v) is list else v))
    return dict(items)
        
def persist_messages(delimiter, quotechar, messages):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}

    logger.info("do persists")

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    header_writes = {}

    for message in messages:
        try:
            o = singer.parse_message(message).asdict()
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(message))
            raise
        message_type = o['type']
        if message_type == 'RECORD':
            if o['stream'] not in schemas:
                raise Exception("A record for stream {}"
                                "was encountered before a corresponding schema".format(o['stream']))

            validators[o['stream']].validate(o['record'])

            filename = o['stream'] + '-' + now + '.csv'

            flattened_record = flatten(o['record'])

            if o['stream'] not in headers:                          
                headers[o['stream']] = extract_header_names(property = schemas[o['stream']]['properties'])
                logger.info(f"generated headers: {headers[o['stream']]}")

            writer = csv.DictWriter(sys.stdout,
                                    headers[o['stream']],
                                    extrasaction='ignore',
                                    delimiter=delimiter,
                                    quotechar=quotechar)
            if o['stream'] not in header_writes:
                header_writes[o['stream']] = True
                writer.writeheader()

            for header in headers[o['stream']]:
                if header not in flattened_record:
                    flattened_record[header] = None
                else:
                    if isinstance(flattened_record[header], str):
                        flattened_record[header] = flattened_record[header].encode("unicode_escape").decode("utf-8")

        

            writer.writerow(flattened_record)

            state = None
        elif message_type == 'STATE':
            logger.debug('Setting state to {}'.format(o['value']))
            state = o['value']
        elif message_type == 'SCHEMA':
            stream = o['stream']
            schemas[stream] = o['schema']
            validators[stream] = Draft4Validator(o['schema'])
            key_properties[stream] = o['key_properties']
        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(o['type'], o))

    return state


def send_usage_stats():
    try:
        version = pkg_resources.get_distribution('target-csv').version
        conn = http.client.HTTPConnection('collector.singer.io', timeout=10)
        conn.connect()
        params = {
            'e': 'se',
            'aid': 'singer',
            'se_ca': 'target-csv',
            'se_ac': 'open',
            'se_la': version,
        }
        conn.request('GET', '/i?' + urllib.parse.urlencode(params))
        response = conn.getresponse()
        conn.close()
    except:
        logger.debug('Collection request failed')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    parser.add_argument('-d', '--delimiter', help='Delimiter character')
    parser.add_argument('-q', '--quotechar', help='Quote character')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input_json:
            config = json.load(input_json)
    else:
        config = {}

    logger.info(f"args: {args}")

    if args.delimiter:
        config['delimiter'] = args.delimiter
    if args.quotechar:
        config['quotechar'] = args.quotechar

    if not config.get('disable_collection', False):
        logger.info('Sending version information to singer.io. ' +
                    'To disable sending anonymous usage data, set ' +
                    'the config parameter "disable_collection" to true')
        threading.Thread(target=send_usage_stats).start()

    logger.info(f"config: {config}")

    input_messages = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_messages(config.get('delimiter', ','),
                             config.get('quotechar', '"'),                             
                             input_messages)

    #emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()

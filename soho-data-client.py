
import argparse
import json
import logging
import requests

LOG = logging.getLogger('soho-data-client')
logging.basicConfig(level=logging.INFO)

if __name__ == '__main__':  

    ap = argparse.ArgumentParser(description='Client to pull SOHO data by indicated time and instruments.')
    ap.add_argument('-d', '--debug', default = False, action = 'store_true', help='Turn on debugging messages')
    ap.add_argument('-c2', default = False, action = 'store_true', help='Download C2 data')
    ap.add_argument('-c3', default = False, action = 'store_true', help='Download C3 data')
    ap.add_argument('-y', '--year', type=int, help='Year to pull data for (YYYY format).', required=True)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    pull_data (args.year, args.c2, args.c3)

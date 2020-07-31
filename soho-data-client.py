
'''
SOHO LASCO Data client.

Returns Level 1 data for selected dates.

'''
import argparse
import calendar
import datetime
import json
import logging
import requests

LOG = logging.getLogger('soho-data-client')
logging.basicConfig(level=logging.INFO)

def pull_soho_data (location:str, month:int, year:int, instrument:str, overwrite:bool=False)->str:
    '''
    Download data from SOHO.
    '''

    report = ""

    BASE_URL = 'https://lasco-www.nrl.navy.mil/lz/level_1'
    # formula for url to pull a file:
    # <BASE_URL>/<DATE>/<INSTRUMENT>/<File> 
    # instrument: "c2" or "c3"
    # date format: YRMMDD ex. '170101'

    # determine days in month for the given year
    download_date = datetime.datetime(year, month, 1)
    lastday = calendar.monthrange(download_date.year, download_date.month)[1]

    # fix format to last 2 digits for year
    if year >= 2000:
        year = args.year - 2000
    else:
        year = args.year - 1900

    # 1. Construct base URL for each day, loop 
    for day in range(1,lastday+1):

        # get date string
        #day = _reformat_val(day)
        #date = f"%2d%2d%2d" % {year}{month}{day}"
        date = "{:02d}{:02d}{:02d}".format(year,month,day)

        # construct url for this date
        url = f"{BASE_URL}/{date}/{instrument}/"  
        print (url)

    # 2. Validation checks
    #  check if location exists, if not bail
    #  check if year is greater than 1995 and less than current year

    # 3. Download data
    #  Loop over all months, days for data in that year
    #  Get list of files for instrument on date
    #  Check if file exists before pulling unless overwrite set
    #  Pull file(s)

    # 3. Return (success) report as string (throw exception for failures)

    return report


if __name__ == '__main__':  

    ap = argparse.ArgumentParser(description='Client to pull SOHO data by indicated time and instruments.')
    ap.add_argument('location', help='Directory/path to download data to')
    ap.add_argument('-d', '--debug', default = False, action = 'store_true', help='Turn on debugging messages')
    ap.add_argument('-c2', default = False, action = 'store_true', help='Download C2 data')
    ap.add_argument('-c3', default = False, action = 'store_true', help='Download C3 data')
    ap.add_argument('-o', '--overwrite', default = False, action = 'store_true', help='Overwrite existing data locally with downloaded files.')
    ap.add_argument('-m', '--month', type=int, help='Year to pull data for (MM format).', required=True)
    ap.add_argument('-y', '--year', type=int, help='Year to pull data for (YYYY format).', required=True)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    if args.c2:
        instrument="c2"
    elif args.c3:
        instrument="c3"
    else:
        LOG.fatal("You need to pick either -c2 or -c3 option")
        exit()
    
    if args.year < 1996:
        LOG.fatal("You cannot specify a year less than 1996")
        exit()

    now = datetime.datetime.now()
    if args.year > now.year:
        LOG.fatal(f"You cannot specify a year greater than %s" % now.year)
        exit()

    pull_soho_data (args.location, args.month, args.year, instrument, args.overwrite)


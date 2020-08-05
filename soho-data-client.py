
'''
SOHO Data download client.

Currently it supports only level 1 LASCO C2, C3 data.

'''
import argparse
import bs4
import calendar
import concurrent.futures 
import datetime
import json
import logging
import os
import requests

from bs4 import BeautifulSoup

LOG = logging.getLogger('soho-data-client')
logging.basicConfig(level=logging.INFO)

# default number of download threads
DEF_NUM_THREADS = 8

# default timeout for GET request in sec
DEF_TIMEOUT = 5

def _parse_table_of_files(table:bs4.element.Tag)->list:
    files = []

    # find all cells with link
    cells = table.find_all('td')
    for td in cells:
        for link in td.find_all('a'):
            file_name = link.attrs['href']
            if 'fts' in file_name or 'fits' in file_name:
                files.append(file_name)
    return files

def download_file(file_url:str, path_to_write_to:str)->list:
    ''' Download a single file from indicated url to location.
    '''
    LOG.debug(f" downloading: {file_url}")
    try:
        file_request = requests.get(file_url)

        with open(path_to_write_to, 'wb') as f:
            f.write(file_request.content)

        return 0, f"Wrote {path_to_write_to}"

    except Exception as ex:
        return 1, f"Failed {path_to_write_to}, exception: {ex}"

def get_page_data(page_url:str, location:str, overwrite:bool=False, timeout:int=DEF_TIMEOUT)->dict:

    statuses = {'success':[], 'skip':[], 'exception':[] }
    LOG.debug(f"Pulling data from {page_url}")
    page = requests.get(page_url, timeout=timeout)

    # marshall page into bs4 soup obj for parsing
    soup = BeautifulSoup(page.text, 'html.parser')

    # Get a list of files for instrument on date
    tables = soup.find_all('table')
    file_info = []
    for fits_file in _parse_table_of_files(tables[0]):
        path_to_write_to = os.path.join(location, fits_file)

        # Check if file exists before pulling unless overwrite set
        if not overwrite and os.path.exists(path_to_write_to):
            msg = f"Skipped {path_to_write_to}, exists"
            statuses['skip'].append(msg)
            LOG.debug(msg)
        else:
            # construct file url and pull via GET request
            info = {'url': f"{page_url}{fits_file}", 'path':path_to_write_to } 
            file_info.append(info)

    for fi in file_info:
        status, msg = download_file(fi['url'], fi['path'])
        if status>0:
            statuses['exception'].append(msg)
        else:
            statuses['success'].append(msg)
        
    return statuses 

def pull_soho_data (location:str, month:int, year:int, instrument:str,\
                    num_threads:int=DEF_NUM_THREADS, overwrite:bool=False)->dict:
    '''
    Download data from SOHO.
    '''
    status = {'file_success':[], 'file_skip':[], 'file_exception':[], 'page_exception':[]}

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
    page_urls = []
    for day in range(1,lastday+1):

        # get date string
        date = "{:02d}{:02d}{:02d}".format(year,month,day)

        # construct url for this date and pull the page
        page_url = f"{BASE_URL}/{date}/{instrument}/"  

        page_urls.append(page_url)

    # TODO: REMOVE AFTER TESTING! Truncate to 10 pages
    page_urls = page_urls[:10]

    # thread on page and download all files found on page within the thread.
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_threads) as executor:

        future_to_url = { executor.submit(get_page_data, page_url, location, overwrite, DEF_TIMEOUT): page_url for page_url in page_urls}
        for future in concurrent.futures.as_completed(future_to_url):
            url = future_to_url[future]
            try:
                page_status = future.result()
                status['file_success'].extend(page_status['success'])
                status['file_skip'].extend(page_status['skip'])
                status['file_exception'].extend(page_status['exception'])
            except Exception as exc:
                LOG.error('Page: %r generated an exception: %s' % (url, exc))
                status['page_exception'].append(exc)

    # 3. Return (success) report as string (throw exception for failures)
    return status


if __name__ == '__main__':  

    ap = argparse.ArgumentParser(description='Client to pull SOHO data by indicated time and instrument.')
    ap.add_argument('location', help='Directory/path to download data to')
    ap.add_argument('-d', '--debug', default = False, action = 'store_true', help='Turn on debugging messages')
    ap.add_argument('-c2', default = False, action = 'store_true', help='Download C2 data')
    ap.add_argument('-c3', default = False, action = 'store_true', help='Download C3 data')
    ap.add_argument('-t', '--num_threads', type=int, help=f'Number of threads to use. Default:{DEF_NUM_THREADS}', default=DEF_NUM_THREADS)
    ap.add_argument('-o', '--overwrite', default = False, action = 'store_true', help='Overwrite existing data locally with downloaded files.')
    ap.add_argument('-m', '--month', type=int, help='Year to pull data for (MM format).', required=True)
    ap.add_argument('-y', '--year', type=int, help='Year to pull data for (YYYY format).', required=True)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    # validation checks

    # did we select an instrument? 
    if args.c2:
        instrument="c2"
    elif args.c3:
        instrument="c3"
    else:
        LOG.fatal(" You need to pick either -c2 or -c3 option")
        exit()
    
    # is the date sane?
    # before 1996 there is no data
    if args.year < 1996:
        LOG.fatal(" You cannot specify a year less than 1996")
        exit()

    # after today there is no data (the future!)
    now = datetime.datetime.now()
    if args.year > now.year:
        LOG.fatal(f" You cannot specify a year greater than %s" % now.year)
        exit()

    #check the location exists
    if not os.path.isdir(args.location):
        LOG.fatal(f" %s directory does not exist (or is not a directory), please fix" % args.location)
        exit()

    #is location writable?
    if not os.access(args.location, os.W_OK): 
        LOG.fatal(f" %s directory is not writable" % args.location)
        exit()

    # pull the data
    statuses = pull_soho_data (args.location, args.month, args.year, instrument, args.num_threads, args.overwrite)

    LOG.info(f"Wrote %s files" % len(statuses['file_success']))
    LOG.info(f"Skipped over %s files (no overwrite)" % len(statuses['file_skip']))
    LOG.error(f" Errors for %s files (exception thrown)" % len(statuses['file_exception']))
    LOG.fatal(f" Error for %s pages (exception thrown)" % len(statuses['page_exception']))


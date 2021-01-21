
'''
SOHO Data download client.

Currently it supports only level 1 LASCO C2, C3 data.

'''
import argparse
import calendar
import concurrent.futures
import datetime
import json
import logging
import os
import pandas as pd
import requests

from pathlib import Path

LOG = logging.getLogger('soho-data-client')
logging.basicConfig(level=logging.INFO)

# default number of download threads
DEF_NUM_THREADS = 8

# default timeout for GET request in sec
DEF_TIMEOUT = 5

# Minimum expected FITS file size threshold, in bytes
MIN_FILE_SIZE = 10000

def download_file(file_url:str, path_to_write_to:str)->list:
    """ 
    Download a single file from indicated url to location.
    """ 

    try:
        LOG.debug(f"Pulling data from {file_url}")
        file_request = requests.get(file_url)

        if len(file_request.content) < MIN_FILE_SIZE:
            # Oops. We didnt get much data back. This can happen because
            # we got redirected because the file is actually gzipped
            # but the name provided to us is not (NRL inconsistency in catalog)
            # lets re-try
            file_url += '.gz'

            LOG.debug(f"Original URL bogus, re-request from {file_url}")
            file_request = requests.get(file_url)

        with open(path_to_write_to, 'wb') as f:
            f.write(file_request.content)

        return 0, f"Wrote {path_to_write_to}"

    except Exception as ex:

        LOG.fatal(f"EXCEPTION: %s" % type(ex))
        return 1, f"Failed {path_to_write_to}, exception: {ex}"


def get_data(idname:str, file_list:list, location:str, overwrite:bool=False, timeout:int=DEF_TIMEOUT, is_level1:bool=True)->dict:

    BASE_URL = 'https://lasco-www.nrl.navy.mil/lz'

    if is_level1:
        BASE_URL += '/level_1'

    statuses = {'success':[], 'skip':[], 'exception':[] }

    download_list = []
    for fits_file in file_list:

        path_to_write_to = os.path.join(location, fits_file)

        # Check if file exists before pulling unless overwrite set
        if not overwrite and os.path.exists(path_to_write_to):
            msg = f"Skipped {path_to_write_to}, exists"
            statuses['skip'].append(msg)
            LOG.debug(msg)

        else:

            # make the path to write to
            Path(os.path.dirname(path_to_write_to)).mkdir(parents=True, exist_ok=True)

            # construct file url and pull via GET request
            info = {'url': f"{BASE_URL}/{fits_file}", 'path':path_to_write_to }
            download_list.append(info)

    for fi in download_list:
        status, msg = download_file(fi['url'], fi['path'])
        if status>0:
            statuses['exception'].append(msg)
        else:
            statuses['success'].append(msg)

    return statuses

# function to get unique values
def unique(list1):

    # insert the list to the set
    list_set = set(list1)

    # convert the set to the list
    # and return
    return list(list_set)

def download_info(id_name:str, filelist:pd.DataFrame)->dict:
    ''' Construct file urls to download from using a passed list of filenames and dates of observation for given telescope. '''


    # split up by indicated id_name column
    id_list = unique(filelist[id_name])
    urls = { idname:[] for idname in id_list }

    for row in filelist.iterrows():

        telescope = row[1]['telescope'].lower() 
        filename = row[1]['filename']
        idn_val = row[1][id_name]

        dt_str = row[1]['datetime'].split()[0]
        date_str = dt_str.split("-")
        year = int(date_str[0])
        month = int(date_str[1])
        day = int(date_str[2])

        # fix format to last 2 digits for year
        if year >= 2000:
            year = year - 2000
        else:
            year = year - 1900

        # get date string
        date = "{:02d}{:02d}{:02d}".format(year,month,day)

        # construct url for this date and pull the page
        url = f"{date}/{telescope}/{filename}"

        urls[idn_val].append(url)

    return urls


def pull_soho_data (id_column:str, location:str, filelist:pd.DataFrame, num_threads:int=DEF_NUM_THREADS, overwrite:bool=False)->dict:
    '''
    Download data from SOHO from a passed list of files.
    '''
    status = {'file_success':[], 'file_skip':[], 'file_exception':[], 'page_exception':[]}

    # formula for url to pull a file:
    # <BASE_URL>/<DATE>/<INSTRUMENT>/<File>
    # telescope: "c2" or "c3"
    # date format: YYMMDD ex. '170101'

    # 1. Construct a download URL for each file
    download_list = download_info(id_column, filelist)

    # thread on groups of files (by id) and download 
    with concurrent.futures.ThreadPoolExecutor(max_workers = num_threads) as executor:

        future_to_url = { executor.submit(get_data, idn, file_list, location, overwrite, DEF_TIMEOUT): file_list for idn, file_list in download_list.items()}
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

    ap = argparse.ArgumentParser(description='Client to pull SOHO data from NRL.')
    ap.add_argument('location', help='Directory/path to download data to')
    ap.add_argument('-d', '--debug', default = False, action = 'store_true', help='Turn on debugging messages')
    ap.add_argument('-id', '--id_column', type=str, help=f'Name of the column to use as an id to group downloaded data.', required=True)
    ap.add_argument('-t', '--num_threads', type=int, help=f'Number of threads to use. Default:{DEF_NUM_THREADS}', default=DEF_NUM_THREADS)
    ap.add_argument('-o', '--overwrite', default = False, action = 'store_true', help='Overwrite existing data locally with downloaded files.')
    ap.add_argument('-f', '--filelist', type=str, help='File in CSV format containing a list of files to download.', required=True)

    args = ap.parse_args()

    if args.debug:
        logging.basicConfig(level=logging.DEBUG)
        LOG.setLevel(logging.DEBUG)

    # validation checks

    #check the location exists
    if not os.path.isdir(args.location):
        LOG.fatal(f" %s directory does not exist (or is not a directory), please fix" % args.location)
        exit()

    #is location writable?
    if not os.access(args.location, os.W_OK):
        LOG.fatal(f" %s directory is not writable" % args.location)
        exit()

    # open our list and get files
    files = pd.read_csv(args.filelist)

    # pull the data
    statuses = pull_soho_data (args.id_column, args.location, files, args.num_threads, args.overwrite)

    LOG.info(f"Wrote %s files" % len(statuses['file_success']))
    LOG.info(f"Skipped over %s files (no overwrite)" % len(statuses['file_skip']))
    LOG.error(f" Errors for %s files (exception thrown)" % len(statuses['file_exception']))
    LOG.fatal(f" Error for %s pages (exception thrown)" % len(statuses['page_exception']))

    if args.debug:
        LOG.error("Got the following unquire file error messages")
        for err_msg in unique(statuses['file_exception']):
            LOG.error(f" * {err_msg}")

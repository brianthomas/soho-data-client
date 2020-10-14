# SOHO Data Client

## About
Simple client for downloading LASCO C2, C3 data from SOHO. It will download data listed in a file to local folders.
You can build these files using the cdaw\_cme\_catalog\_analysis repository.

## Installation
```bash
> python3 -m venv ./venv
> source venv/bin/activate
```

## Usage
```bash
usage: soho-data-client.py [-h] [-d] [-t NUM_THREADS] [-o] -f FILELIST location

Client to pull SOHO data from NRL.

positional arguments:
  location              Directory/path to download data to

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Turn on debugging messages
  -t NUM_THREADS, --num_threads NUM_THREADS
                        Number of threads to use. Default:8
  -o, --overwrite       Overwrite existing data locally with downloaded files.
  -f FILELIST, --filelist FILELIST
                        File in CSV format containing a list of files to download.

``` 

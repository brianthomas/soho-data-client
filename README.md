# SOHO Data Client

## About
Simple client for downloading LASCO C2, C3 data from SOHO. It will download data on a month-by-month basis to a local folder.  

## TODO:

fold in the master image hdr file + a selection of time intervals to be able to cherry pick the files we wish to have.

master image hdr file: https://lasco-www.nrl.navy.mil/lz/img_hdr.txt (its large!) 

## Installation
```bash
> python3 -m venv ./venv
> source venv/bin/activate
```

## Usage
```bash
usage: soho-data-client.py [-h] [-d] [-c2] [-c3] [-t NUM_THREADS] [-o] -m
                           MONTH -y YEAR
                           location

Client to pull SOHO data by indicated time and instrument.

positional arguments:
  location              Directory/path to download data to

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Turn on debugging messages
  -c2                   Download C2 data
  -c3                   Download C3 data
  -t NUM_THREADS, --num_threads NUM_THREADS
                        Number of threads to use. Default:8
  -o, --overwrite       Overwrite existing data locally with downloaded files.
  -m MONTH, --month MONTH
                        Year to pull data for (MM format).
  -y YEAR, --year YEAR  Year to pull data for (YYYY format).
``` 

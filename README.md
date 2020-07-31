# SOHO Data Client

## About
Simple client for downloading LASCO C2, C3 data from SOHO

## Installation


## Usage
```bash
usage: soho-data-client.py [-h] [-d] [-c2] [-c3] [-o] -m MONTH -y YEAR
                           location

Client to pull SOHO data by indicated time and instrument.

positional arguments:
  location              Directory/path to download data to

optional arguments:
  -h, --help            show this help message and exit
  -d, --debug           Turn on debugging messages
  -c2                   Download C2 data
  -c3                   Download C3 data
  -o, --overwrite       Overwrite existing data locally with downloaded files.
  -m MONTH, --month MONTH
                        Year to pull data for (MM format).
  -y YEAR, --year YEAR  Year to pull data for (YYYY format).
``` 

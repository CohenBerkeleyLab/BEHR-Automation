from __future__ import print_function

from . import automodis as am

import argparse
import datetime as dt
from glob import glob
import os
import re
import shutil
import sys

import pdb

modis_date_re = re.compile('(?<=A)\d{7}')
default_min_start_date = dt.datetime.today() - dt.timedelta(days=90)
behr_start_date = dt.datetime(2005, 1, 1)
max_download_attempts = 10
USERAGENT = 'tis/download.py_1.0--' + sys.version.replace('\n','').replace('\r','')

def get_product_last_date(product, path, min_start_date=None, verbose=0):
    """
    Searches the directory given by path to find yearly subdirectories and finds
    the most recent file for product, assuming that the files start with the
    product name
    """
    if min_start_date is None:
        min_start_date = default_min_start_date


    year_subdirs = reversed(sorted(glob(os.path.join(path, '20*'))))
    for last_year in year_subdirs:
        if verbose > 1:
            print('Searching {}'.format(last_year))
        product_files = sorted(glob(os.path.join(last_year, '{}*'.format(product))))
        if len(product_files) == 0:
            continue

        most_recent_file = os.path.basename(product_files[-1])
        most_recent_datestr = modis_date_re.search(most_recent_file).group()
        return min(dt.datetime.strptime(most_recent_datestr, '%Y%j'), min_start_date)

    # If we don't find any files for the given product, we'd need to download all of those files from the beginning of
    # the time period we process BEHR for.
    return behr_start_date


def list_product_urls(product, collection, path, min_start_date=None, verbose=0):
    first_download_date = get_product_last_date(product, path, min_start_date=min_start_date, verbose=verbose) + dt.timedelta(days=1)
    file_urls = am.get_modis(products=product, collection=collection, startTime=first_download_date.strftime('%Y-%m-%d %H:%M:%S'),
                             endTime=dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), dayNightBoth='DB')
    if file_urls is None:
        raise RuntimeError('No file URLs obtained')
    else:
        return file_urls


def get_earthdata_token():
    with open(os.path.join(os.path.expanduser('~'), '.earthdata-app-key'), 'r') as fobj:
        for line in fobj:
            if line.startswith('#'):
                continue
            else:
                return line.strip()


def geturl(url, token=None, out=None, verbose=0):
    """
    Retrieves a file from the MODIS LAADS server. Modified from
    https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/#python
    """
    headers = { 'user-agent' : USERAGENT }
    if not token is None:
        headers['Authorization'] = 'Bearer ' + token
    safety = 0
    while safety < max_download_attempts:
        safety += 1
        if safety > 1 and verbose > 0:
            print('   Retrying download for {}'.format(url))

        try:
            import ssl
            CTX = ssl.SSLContext(ssl.PROTOCOL_TLSv1_2)
            if sys.version_info.major == 2:
                import urllib2
                try:
                    fh = urllib2.urlopen(urllib2.Request(url, headers=headers), context=CTX)
                    if out is None:
                        return fh.read()
                    else:
                        shutil.copyfileobj(fh, out)
                except urllib2.HTTPError as e:
                    print('HTTP GET error code: %d' % e.code, file=sys.stderr)
                    print('HTTP GET error message: %s' % e.message, file=sys.stderr)
                except urllib2.URLError as e:
                    print('Failed to make request: %s' % e.reason, file=sys.stderr)
                else:
                    return None

            else:
                from urllib.request import urlopen, Request, URLError, HTTPError
                try:
                    fh = urlopen(Request(url, headers=headers), context=CTX)
                    if out is None:
                        return fh.read().decode('utf-8')
                    else:
                        shutil.copyfileobj(fh, out)
                except HTTPError as e:
                    print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                    print('HTTP GET error message: %s' % e.message, file=sys.stderr)
                except URLError as e:
                    print('Failed to make request: %s' % e.reason, file=sys.stderr)
                else:
                    return None

        except AttributeError:
            # OS X Python 2 and 3 don't support tlsv1.1+ therefore... curl
            # Not modified to try to redownload a failed file (JLL 22 May 2018)
            import subprocess
            try:
                args = ['curl', '--fail', '-sS', '-L', '--get', url]
                for (k,v) in headers.items():
                    args.extend(['-H', ': '.join([k, v])])
                if out is None:
                    # python3's subprocess.check_output returns stdout as a byte string
                    result = subprocess.check_output(args)
                    return result.decode('utf-8') if isinstance(result, bytes) else result
                else:
                    subprocess.call(args, stdout=out)
            except subprocess.CalledProcessError as e:
                print('curl GET error message: %' + (e.message if hasattr(e, 'message') else e.output), file=sys.stderr)
            return None

    # If we get here, the download never completed successfully
    raise RuntimeError('Number of download attempts for {} exceeded the maximum allowed {}'.format(
        url, max_download_attempts
    ))


def download_product(product, collection, path, min_start_date=None, verbose=0):
    token = get_earthdata_token()
    urls = list_product_urls(product, collection, path, min_start_date=min_start_date, verbose=verbose)
    for link in urls:
        file_datestr = modis_date_re.search(link).group()
        file_date = dt.datetime.strptime(file_datestr, '%Y%j')
        year_str = file_date.strftime('%Y')
        year_directory = os.path.join(path, year_str)
        if not os.path.isdir(year_directory):
            os.mkdir(year_directory)

        file_basename = os.path.basename(link)
        file_fullname = os.path.join(year_directory, file_basename)
        if os.path.isfile(file_fullname) and os.path.getsize(file_fullname) > 0:
            if verbose > 1:
                print('File {} already exists and size is > 0; not re-downloaded'.format(file_fullname))
            continue

        with open(file_fullname, 'wb') as save_obj:
            if verbose > 0:
                print('Downloading {} to {}'.format(link, os.path.join(year_directory, file_basename)))
            geturl(link, token, save_obj, verbose=verbose)


def driver(min_start_date=None, verbose=0):
    modis_path = os.getenv('MODDIR')
    modis_alb_dir = os.path.join(modis_path, 'MCD43D')
    modis_cloud_dir = os.path.join(modis_path, 'MYD06_L2')
    modis_alb_collection = '6'
    modis_cloud_collection = '61'
    products = [('MCD43D07', modis_alb_collection, modis_alb_dir),
                ('MCD43D08', modis_alb_collection, modis_alb_dir),
                ('MCD43D09', modis_alb_collection, modis_alb_dir),
                ('MCD43D31', modis_alb_collection, modis_alb_dir),
                ('MYD06_L2', modis_cloud_collection, modis_cloud_dir)]
    for product, collection, path in products:
        if verbose > 0:
            print('Now downloading the {} (collection {}) product'.format(product, collection))
        download_product(product, collection, path, min_start_date=min_start_date, verbose=verbose)


def parse_cl_date(datestr):
    if datestr is None:
        return default_min_start_date
    else:
        return dt.datetime.strptime(datestr, '%Y-%m-%d')


def parse_args():
    parser = argparse.ArgumentParser(description='Driver to download MODIS albedo and cloud data')
    parser.add_argument('-d', '--min-start-date', default=None, type=parse_cl_date,
                        help='The required first date to check if MODIS files exist. This program will start from this '
                             'date or the most recent MODIS file, if the most recent file is before this date. Default '
                             'is 90 days ago.')
    parser.add_argument('-v', '--verbose', action='count', help='Increase verbosity to the terminal')

    return vars(parser.parse_args())


def main():
    args_dict = parse_args()
    driver(**args_dict)


if __name__ == '__main__':
    main()

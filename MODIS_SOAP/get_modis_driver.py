from . import automodis as am

import datetime as dt
from glob import glob
import os
import re

modis_date_re = re.compile('(?<=A)\d{7}')

def get_product_last_date(product, path):
    """
    Searches the directory given by path to find yearly subdirectories and finds
    the most recent file for product, assuming that the files start with the
    product name
    """
    year_subdirs = sorted(glob(os.path.join(path, '20*')))
    last_year = year_subdirs[-1]
    product_files = sorted(glob(os.path.join(last_year, '{}*'.format(product))))
    most_recent_file = os.path.basename(product_files[-1])
    most_recent_datestr = modis_date_re.search(modis_date_re).group()
    return dt.datetime.strptime(most_recent_datestr, '%Y%j')

def list_product_urls(product, collection, path):
    first_download_date = get_product_last_date(product, path) + dt.timedelta(days=1)
    file_urls = am.get_modis(products=product, collection=collection, startTime=first_download_date.strftime('%Y-%m-%d %H:%M:%S'),
                             endTime=dt.datetime.today().strftime('%Y-%m-%d %H:%M:%S'), dayNightBoth='DB')


def get_earthdata_token():
    with open(os.path.join(os.path.expanduser('~'), '.earthdata-app-key'), 'r') as fobj:
        for line in fobj:
            if line.startswith('#'):
                continue
            else:
                return line


def geturl(url, token=None, out=None):
    """
    Retrieves a file from the MODIS LAADS server. Modified from
    https://ladsweb.modaps.eosdis.nasa.gov/tools-and-services/data-download-scripts/#python
    """
    headers = { 'user-agent' : USERAGENT }
    if not token is None:
        headers['Authorization'] = 'Bearer ' + token
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
                print('HTTP GET error code: %d' % e.code(), file=sys.stderr)
                print('HTTP GET error message: %s' % e.message, file=sys.stderr)
            except urllib2.URLError as e:
                print('Failed to make request: %s' % e.reason, file=sys.stderr)
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
            return None

    except AttributeError:
        # OS X Python 2 and 3 don't support tlsv1.1+ therefore... curl
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

def download_product(product, collection, path):
    token = get_earthdata_token()
    urls = list_product_urls(product, collection, path)
    for link in urls:
        file_datestr = modis_date_re.search(link).group()
        file_date = dt.datetime.strptime(file_datestr, '%Y%j')
        year_str = file_date.strftime('%Y')
        year_directory = os.path.join(path, year_str)
        if not os.path.isdir(year_directory):
            os.mkdir(year_directory)

        file_basename = os.basename(link)
        with open(os.path.join(year_directory, file_basename), 'wb') as save_obj:
            geturl(link, token, save_obj)

def main():
    modis_path = os.getenv('MODDIR')
    modis_alb_collection = '6'
    modis_cloud_collection = '61'
    products = [('MCD43D07', modis_alb_collection, modis_path),
                ('MCD43D08', modis_alb_collection, modis_path),
                ('MCD43D09', modis_alb_collection, modis_path),
                ('MCD43D31', modis_alb_collection, modis_path),
                ('MYD06_L2', modis_cloud_collection, modis_path)]
    for product, collection, path in products:
        download_product(product, collection, path)

if __name__ == '__main__':
    main()

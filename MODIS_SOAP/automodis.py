#!/usr/bin/env python3
from __future__ import print_function, absolute_import, division
# This function will call into the MODAPS web services server
# and retrieve a space delimited list of URLs to retrieve, which
# will be returned to the invoking shell if called using the bash
# syntax urls=$(python automodis.py)

from SOAPpy import SOAPProxy
import argparse
import os
import sys
import time
import pdb

default_vals = {'north': 55.0, 'south': 20.0, 'east': -65.0, 'west': -125.0,
                'coordsOrTiles': 'coords', 'dayNightBoth': 'DNB'}


def parse_args():
    """
    Parses command line arguments given in bash. Assumes that all arguments are flag-value pairs
     using long-option nomenclature (e.g. --products MYD06_L2).
    Allows for two types of arguments: required and optional.  Required are specified in the list
     below (req_args) and will cause an error if not present. Optional arguments are given in a
     dictionary; these are arguments that if not specified have reasonable default values, at least
     for implementation for BEHR. The key will be the flag name (without the --) and the value is
     the default value.
    :param args_in: Pass
    :return:
    """

    parser = argparse.ArgumentParser(description="program to retrieve modis_urls")
    parser.add_argument("products", help="modis products to download")
    parser.add_argument("collection", help="modis collection number to download")
    parser.add_argument("startTime", help = "begining of time period for modis data YYYY-MM-DD HH:MM:SS")
    parser.add_argument("endTime", help = "end of time period for modis data YYYY-MM-DD HH:MM:SS")
    parser.add_argument("--north", default = default_vals['north'], type = float, help = "north bound for modis data")
    parser.add_argument("--south", default = default_vals['south'], type = float, help = "south bound for modis data")
    parser.add_argument("--east", default = default_vals['east'], type = float, help = "east bound for modis data")
    parser.add_argument("--west", default = default_vals['west'], type = float, help = "west bound for modis data")
    parser.add_argument("--coordsOrTiles", default = default_vals['coordsOrTiles'], choices = ["tiles","coords"], help = "to download coords or tiles. Default is %(default)s")
    parser.add_argument("--dayNightBoth", default = default_vals['dayNightBoth'], help = "must contain the character 'N','D','B'")
    parser.add_argument("--output-file", default=None, help="the file to write the URLs to")

    args = parser.parse_args()

    return vars(args)

def write_urls(urls, output_file):
    #p = os.environ['MATRUNDIR']
    #filename = os.path.join(p,'modis_urls.txt')
    with open(output_file,'w') as f:
        for l in urls:
            f.writelines(l+"\n")

def get_modis(products, collection, startTime, endTime, north=default_vals['north'], south=default_vals['south'],
              east=default_vals['east'], west=default_vals['west'], dayNightBoth=default_vals['dayNightBoth'],
              coordsOrTiles=default_vals['coordsOrTiles'], output_file=None):

    #url = "http://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices"
    url = "https://modwebsrv.modaps.eosdis.nasa.gov/axis2/services/MODAPSservices"

    server = SOAPProxy(url)
    print("Retrieving file IDs")
    attempt=0
    while True:
        try:
            fileIDs = server.searchForFiles(products=products, collection=collection, startTime=startTime, endTime=endTime,
            north=north, south=south, east=east, west=west, coordsOrTiles=coordsOrTiles, dayNightBoth=dayNightBoth)
        except Exception as err:
            if attempt > 5:
                print("More than five attempts failed to retrieve file IDs. Aborting.")
                raise
            else:
                print("Retrieving file IDs failed, waiting 30 sec")
                print("Message was:", str(err))
                time.sleep(30)
        else:
            break
        finally:
            attempt += 1

    if fileIDs == 'No results':
        if output_file is None:
            return
        else:
            exit(2)

    print("fileIDs has length", len(fileIDs))
    fileIDs = ",".join(fileIDs) # returned as list, need as comma separated string

    attempt=0
    while True:
        try:
            fileURLs = server.getFileUrls(fileIds=fileIDs)
        except Exception as err:
            if attempt > 5:
                print("More than five attempts failed to retrieve file URLs. Aborting.")
                raise
            else:
                print("Retrieving file URLs failed, waiting 30 sec")
                print("Message was:", str(err))
                time.sleep(30)
        else:
            break
        finally:
            attempt += 1

    #fileURLs = "\n".join(fileURLs)

    # Write the URLs to a text file in the directory defined by the MATRUNDIR environmental
    # variable
    if output_file is not None:
        write_urls(fileURLs)
    else:
        return fileURLs


if __name__ == '__main__':
    args = parse_args()
    get_modis(**args)
    exit(0)

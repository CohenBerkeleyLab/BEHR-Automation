#!/usr/bin/env python3

from __future__ import print_function

import argparse
import os

website_root_dir = 'webData'
website_url = 'http://behr.cchem.berkeley.edu/behr/'


def make_full_url(path):
    # Strip everything up to the website root directory
    path = os.path.abspath(os.path.realpath(path))
    path_split = path.split(os.path.sep)
    if website_root_dir not in path_split:
        raise ValueError('Cannot find website root directory ("{}") in path "{}"'.format(website_root_dir, path))
    root_idx = path_split.index(website_root_dir)
    return website_url + '/'.join(path_split[root_idx+1:])


def parse_args():
    parser = argparse.ArgumentParser(description='Generate manifest list of URLs for BEHR files')
    parser.add_argument('files', nargs='+', help='The files to make the list for')

    return parser.parse_args()


def main(files):
    for f in files:
        print(make_full_url(f))


if __name__ == '__main__':
    args = parse_args()
    main(args.files)


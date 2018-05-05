#!/usr/bin/env python3

from __future__ import print_function

import argparse
import datetime as dt
from glob import glob
import multiprocessing as mp
import os
import re
import sys
import tarfile


def som_date(date):
    return date.replace(day=1)


def eom_date(date):
    # credit to https://stackoverflow.com/a/13565185
    next_month = date.replace(day=28) + dt.timedelta(days=4)  # this will never fail
    return next_month - dt.timedelta(days=next_month.day)


def parse_datearg(date_str, end_of_month):
    d = dt.datetime.strptime(date_str, '%Y-%m')
    if end_of_month:
        d = eom_date(d)
    return d


def iter_months(behr_path, start, end, require_all_days_of_month=False, verbosity=0):
    curr_date = som_date(start)
    all_behr_files = glob(os.path.join(behr_path, '*.hdf'))
    behr_date_re = re.compile('\d{8}')
    all_behr_dates = [dt.datetime.strptime(behr_date_re.search(f).group(), '%Y%m%d') for f in all_behr_files]
    while curr_date <= end:
        files_to_tar = sorted([all_behr_files[i] for i in range(len(all_behr_files)) if curr_date <= all_behr_dates[i] <= eom_date(curr_date)])

        if require_all_days_of_month:
            do_tarring = len(files_to_tar) == eom_date(curr_date).day
        else:
            do_tarring = len(files_to_tar) > 0

        if do_tarring:
            tar_name, _ = os.path.splitext(os.path.basename(behr_date_re.sub(som_date(curr_date).strftime('%Y%m'), files_to_tar[0])))
            tar_name += '.tar'
            yield files_to_tar, tar_name
        elif verbosity > 0:
            if require_all_days_of_month:
                print('Skipping {} because fewer than {} files available'.format(curr_date.strftime('%b %Y'), eom_date(curr_date).day), file=sys.stderr)
            else:
                print('Skipping {} because no files available'.format(curr_date.strftime('%b %Y')), file=sys.stderr)

        curr_date = eom_date(curr_date) + dt.timedelta(days=1)


def parse_args():
    parser = argparse.ArgumentParser(description='Collect BEHR files into monthly tar files')
    parser.add_argument('-c', '--compression', choices=['none','gzip', 'bzip2'], default='none', help='Compression algorithm to use. Default is "%(default)s".')
    parser.add_argument('-o', '--outdir', default='.', help='Directory to place the generated archive files into. Default is "%(default)s".')
    parser.add_argument('-s', '--skip-incomplete-months', action='store_true', help='Skip months that do not have a file for every day')
    parser.add_argument('-p', '--parallel', action='store_true', help='Run this in parallel, using as many cores as possible')
    parser.add_argument('-v', '--verbose', action='count', help='Increase output to terminal')
    parser.add_argument('start', type=lambda s: parse_datearg(s, False), help='Starting date to combine into a tar file in yyyy-mm format')
    parser.add_argument('end', type=lambda s: parse_datearg(s, True), help='Starting date to combine into a tar file in yyyy-mm format')
    parser.add_argument('path', help='The path to the directory containing the archive files')

    return parser.parse_args()


def make_tar_file(files, tarname, args):
    tarmodes = {'none': 'w', 'gzip': 'w:gz', 'bzip2': 'w:bz2'}
    tarext = {'none': '', 'gzip': '.gz', 'bzip2': '.bz2'}
    full_tarname = os.path.join(args.outdir, tarname + tarext[args.compression])
    with tarfile.open(full_tarname, tarmodes[args.compression]) as tarobj:
        for f in files:
            if args.verbose > 1:
                print('Adding {} to {}'.format(f, full_tarname))
            # remove whatever extension there is. os.path.splitext doesn't work because e.g. .tar.gz looks like two
            # extensions and so doesn't get completely removed.
            tar_inner_dir = re.sub('\.tar(\.gz)?(\.bz2)?', '', os.path.basename(full_tarname))
            # Not sure how tar behaves if you try to use \ as path separators, so I'm going to specify joining the
            # inner directory to the file names with a / instead of using os.path.join(). Specifying a path like
            # this *should* prevent "tar bombing" where a huge number of files are put in the user's current
            # directory when the untar something.
            tarobj.add(f, arcname=tar_inner_dir + '/' + os.path.basename(f))


def main():
    args = parse_args()
    par_args = [(files, tarname, args) for files, tarname in
                iter_months(args.path, args.start, args.end,
                            require_all_days_of_month=args.skip_incomplete_months,
                            verbosity=args.verbose)]
    if args.parallel:
        with mp.Pool() as pool:
            pool.starmap(make_tar_file, par_args)
    else:
        for files, tarname, _ in par_args:
            make_tar_file(files, tarname, args)


if __name__ == '__main__':
    main()

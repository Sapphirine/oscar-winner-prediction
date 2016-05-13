""" Filter new hourly pageview count raw gzips and reduce to daily counts. """

from __future__ import division
import fnmatch
import json
import gzip
import logging
import os
from time import time

from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CACHE_FILE = "../cache/pv/pageviews.gz"
RAW_PAGEVIEWS_FOLDER = "/Volumes/TempDrive/pageviews/"
WIKI_TITLES = "wiki_titles.json"


def load_wiki_titles(path):
    with open(path, 'rb') as fi:
        wt = json.load(fi)
    # Add generalized title names, e.g.
    # 'The_Theory_of_Everything_(2014_film)'
    #       ==> 'The_Theory_of_Everything'
    #       ==> 'The_Theory_of_Everything_%282014_film%29'
    # 'The_Imitation_Game' ==> 'The_Imitation_Game'
    titles = []
    for k, v in wt.items():
        lparen_idx = v.rfind('(')
        titles.append(v)
        if lparen_idx > -1:
            titles.append(v.replace('(', '%28').replace(')', '%29'))
        if v == u'Les_Mis\xe9rables_(2012_film)':
            titles.append(u'Les_Mis%C3%A9rables_(2012_film)')
            titles.append(u'Les_Miserables_(2012_film)')
        if v == u'Frost/Nixon_(film)':
            titles.append(u'Frost%2FNixon_(film)')
        if v == u'Mad_Max:_Fury_Road':
            titles.append(u'Mad_Max%3A_Fury_Road')
    return sorted(titles)


# Workaround for loading entire textfiles into memory:
# http://stackoverflow.com/questions/36258241/using-wholetextfiles-in-pyspark-but-get-the-error-of-out-of-memory
def read_fun_generator(filename):
    with gzip.open(filename, 'rb') as f:
        filename = os.path.basename(filename).encode('utf-8')
        for line in f:
            yield line.decode('latin-1').strip() + u'|' + filename


def gather_filepaths(folder, pattern='*'):
    files = []
    for root, _, fns in os.walk(folder):
        for f in fnmatch.filter(fns, pattern):
            p = os.path.join(root, f)
            files.append(p)
    return files


def filter_raws(titles):
    t0 = time()
    prefixes = set([u'en ' + t for t in titles])
    folder = os.path.split(CACHE_FILE)[0]
    if not os.path.exists(folder):
        os.makedirs(folder)
    n = 0

    pattern = 'pagecounts-*.gz'
    files = gather_filepaths(RAW_PAGEVIEWS_FOLDER, pattern)
    total = len(files)
    logger.info('Processing %d files..' % total)
    fo = gzip.open(CACHE_FILE, 'ab')
    i = 1
    for f in files:
        logger.info('Input %d/%d: %s' % (i, total, os.path.basename(f)))
        for line in tqdm(read_fun_generator(f), leave=False):
            if any([line.startswith(p) for p in prefixes]):
                fo.write(line.encode('utf-8'))
                fo.write('\n')
                n += 1
        i += 1
    fo.close()
    logger.info('Appended %d lines to: %s' % (n, CACHE_FILE))

    for f in files:
        os.remove(f)
    logger.info('Deleted input files')
    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)


if __name__ == '__main__':
    titles = load_wiki_titles(WIKI_TITLES)
    filter_raws(titles)

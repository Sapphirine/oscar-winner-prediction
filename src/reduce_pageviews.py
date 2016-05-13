""" Filter new hourly pageview count raw gzips and reduce to daily counts. """

from __future__ import division
import csv
import fnmatch
import json
import gzip
import logging
import os
from time import time

from pyspark import SparkContext

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

RAW_PAGEVIEWS_FOLDER = "/Volumes/GitDocs/pageviews/other/pagecounts-raw/"
CACHE_FILE = "../cache/pv/pageviews.gz"
COUNTS_FILE = "../cache/viewsperday.csv"
WIKI_TITLES = "/Users/Rich/Documents/CS/EECS-E6895-ABDA/project/src/wiki_titles.json"


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


def filter_raws(sc, titles):
    t0 = time()
    prefixes = [u'en ' + t for t in titles]

    pattern = 'pagecounts-*.gz'
    files = gather_filepaths(RAW_PAGEVIEWS_FOLDER, pattern)
    files = sc.parallelize(files)
    logger.info('Processing %d files..' % files.count())
    for f in files.toLocalIterator():
        logger.info('Input: %s' % (os.path.basename(f)))

    rdd = files.flatMap(read_fun_generator)
    rdd = rdd.filter(lambda x: any([x.startswith(p) for p in prefixes]))

    folder = os.path.split(CACHE_FILE)[0]
    if not os.path.exists(folder):
        os.makedirs(folder)
    n = 0
    with gzip.open(CACHE_FILE, 'ab') as fo:
        for x in rdd.toLocalIterator():
            fo.write(x)
            fo.write('\n')
            n += 1
    logger.info('Appended %d lines to: %s' % (n, CACHE_FILE))

    for f in files.toLocalIterator():
        os.remove(f)
    logger.info('Deleted input files')
    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)


def make_count_kv(x):
    line, filename = x.split('|')
    filename = os.path.splitext(filename)[0]
    date = filename.split('-')[1]
    _, title, views, _ = line.split()
    key = date + "|" + title
    return key, int(views)


def count_views(sc, path):
    t0 = time()
    logger.info('Calculating views per day from: %s' % path)
    rdd = sc.textFile(path)
    rdd = rdd.map(make_count_kv)
    rdd = rdd.reduceByKey(lambda x, y: x + y)
    with open(COUNTS_FILE, 'wb') as fo:
        wr = csv.writer(fo)
        wr.writerow(['date', 'title', 'views'])
        for x in rdd.toLocalIterator():
            key, views = x
            date, title = key.split('|')
            row = [date, title.encode('utf-8'), views]
            wr.writerow(row)
    logger.info('Saved: %s' % COUNTS_FILE)
    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)


if __name__ == '__main__':
    sc = SparkContext("local", "Reduce Pageviews")
    titles = load_wiki_titles(WIKI_TITLES)
    filter_raws(sc, titles)
    count_views(sc, CACHE_FILE)

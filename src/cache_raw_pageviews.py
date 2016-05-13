""" Save review pages locally """

import gzip
import json
import logging
import os
from random import random
from time import time, sleep

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__ if __name__ != '__main__' else [])

DATA_FOLDER = '../data/'
CACHE_FOLDER = '../cache/'
DRIVER = '../lib/chromedriver'
PAUSE_BASE = 2.5
PAUSE_VAR = 5
WIKI_TITLES = "wiki_titles.json"

MONTHS = [200712, 200801, 200802, 200803, 200804, 200805, 200806, 200807,
          200808, 200809, 200810, 200811, 200812, 200901, 200902, 200903,
          200904, 200905, 200906, 200907, 200908, 200909, 200910, 200911,
          200912, 201001, 201002, 201003, 201004, 201005, 201006, 201007,
          201008, 201009, 201010, 201011, 201012, 201101, 201102, 201103,
          201104, 201105, 201106, 201107, 201108, 201109, 201110, 201111,
          201112, 201201, 201202, 201203, 201204, 201205, 201206, 201207,
          201208, 201209, 201210, 201211, 201212, 201301, 201302, 201303,
          201304, 201305, 201306, 201307, 201308, 201309, 201310, 201311,
          201312, 201401, 201402, 201403, 201404, 201405, 201406, 201407,
          201408, 201409, 201410, 201411, 201412, 201501, 201502, 201503,
          201504, 201505, 201506, 201507, 201508, 201509, 201510, 201511,
          201512, 201601, 201602]


def json_by_month(driver, title):
    """
    URLs e.g.: http://stats.grok.se/json/en/201510/Argo_(2012_film)

    Output, e.g.:

    {"daily_views": {"2015-10-16": 3705, "2015-10-17": 1772, "2015-10-14": 1506,
                     "2015-10-15": 1762, "2015-10-13": 1880, "2015-10-10": 1485,
                     "2015-10-11": 1852, "2015-10-18": 1940, "2015-10-19": 1978,
                     "2015-10-29": 1411, "2015-10-28": 1457, "2015-10-27": 1556,
                     "2015-10-26": 1501, "2015-10-25": 1469, "2015-10-24": 1410,
                     "2015-10-23": 1452, "2015-10-22": 1493, "2015-10-21": 1605,
                     "2015-10-20": 1654, "2015-10-05": 1623, "2015-10-04": 1406,
                     "2015-10-07": 1494, "2015-10-06": 1897, "2015-10-01": 1378,
                     "2015-10-03": 1585, "2015-10-02": 1428, "2015-10-09": 1464,
                     "2015-10-08": 1330, "2015-10-30": 1334,
                     "2015-10-31": 1184},
     "project": "en",
     "month": "201510",
     "rank": 2034,
     "title": "Argo_(2012_film)"}
    """
    base_url = 'http://stats.grok.se/json/en/'
    for m in MONTHS:
        sleep(PAUSE_VAR * random() + PAUSE_BASE)
        url = base_url + str(m) + '/' + title
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        data = json.loads(soup.body.text)
        yield data


def json_to_lines(j):
    """
    Match json data from pageview API to scripts that process, e.g.:

    en Argo_(2012_film) 63 3096234|pagecounts-20160101-000000.gz
    """
    base_left = j['project'] + u' ' + j['title'] + u' '
    base_mid = u' 0|pagecounts-'
    base_right = u'-999999.gz'
    daily_views = j['daily_views']
    for day, views in daily_views.items():
        day = day.replace('-', '')
        line = base_left + unicode(views) + base_mid + day + base_right
        yield line


def process(driver, wt):
    """
    Parameters
    ----------
    driver : webdriver
    """
    i, n = 1, len(wt)
    folder = CACHE_FOLDER + 'pageviews/'
    logger.info('Processing %d titles..' % n)
    logger.info('Saving files to: %s' % folder)
    if not os.path.exists(folder):
        os.makedirs(folder)
    for id in sorted(wt):
        title = wt[id]
        logger.info('Title %d/%d: %s' % (i, n, title))
        f = folder + id + '.gz'
        fo = gzip.open(f, 'wb')
        for j in tqdm(json_by_month(driver, title), total=len(MONTHS)):
            for line in json_to_lines(j):
                fo.write(line.encode('utf-8'))
                fo.write('\n')
        fo.close()
        # logger.info('Saved: %s' % f)
        i += 1


def main():
    t0 = time()

    with open(WIKI_TITLES, 'rb') as fi:
        wt = json.load(fi)

    driver = webdriver.Chrome(executable_path=DRIVER)
    driver.wait = WebDriverWait(driver, 10)
    try:
        process(driver, wt)
    finally:
        driver.quit()

    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)

if __name__ == '__main__':
    main()

""" Save review pages locally """

import json
import logging
import os
from random import random
from time import time, sleep

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.support.ui import WebDriverWait

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

DATA_FOLDER = '../data/'
CACHE_FOLDER = '../cache/reviews/'
DRIVER = '../lib/chromedriver'
PAUSE_BASE = 3
PAUSE_VAR = 2
PAGES = 'imdb-urls.json'


def get_reviews(driver, begin_url, folder):
    """
    Parameters
    ----------
    driver : webdriver
    """
    xpaths = {'pages': '//*[@id="tn15content"]/table[1]/tbody/tr/td[1]/font'}
    pgs = driver.find_element_by_xpath(xpaths['pages']).text[:-1]
    cur_pg, max_pg = (int(pgs.split()[i]) for i in [1, 3])
    while cur_pg <= max_pg:
        out_path = folder + str(cur_pg) + '.html'
        with open(out_path, 'wb') as fo:
            html = driver.page_source
            fo.write(html.encode('utf-8'))
        logger.info('Saved %s to: %s' % (pgs, out_path))
        if cur_pg < max_pg:
            sleep(PAUSE_VAR * random() + PAUSE_BASE)
            next_url = begin_url + '?start=' + str(10 * cur_pg)
            logger.info('Fetching: %s' % next_url)
            driver.get(next_url)
            pgs = driver.find_element_by_xpath(xpaths['pages']).text[:-1]
            cur_pg = int(pgs.split()[1])
        else:
            break


def process(driver):
    """
    Parameters
    ----------
    driver : webdriver
    """
    logger.info('Loading urls from: %s' % PAGES)
    with open(PAGES, 'rb') as fi:
        start_pages = json.load(fi)
    start_pages = start_pages
    for title in sorted(start_pages):
        sleep(PAUSE_VAR * random() + PAUSE_BASE)
        folder = CACHE_FOLDER + title + '/'
        if not os.path.exists(folder):
            os.makedirs(folder)
        url = start_pages[title]
        logger.info('Fetching: %s' % url)
        driver.get(url)
        soup = BeautifulSoup(driver.page_source, 'lxml')
        title = soup.title.text
        logger.info('Processing: %s' % title)
        get_reviews(driver, url, folder)


def main():
    t0 = time()

    driver = webdriver.Chrome(executable_path=DRIVER)
    driver.wait = WebDriverWait(driver, 10)
    try:
        process(driver)
    finally:
        driver.quit()

    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)

if __name__ == '__main__':
    main()

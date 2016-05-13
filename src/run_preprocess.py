import cPickle
import logging
import os
from time import time

import pandas as pd

from preprocess.oscars import Oscars
from preprocess.imdb import Imdb, ImdbReviews
from preprocess.pagviews import PageViews

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__ if __name__ != '__main__' else [])

DATA_FOLDER = '../data/'
CACHE_FOLDER = '../cache/'


def main():
    t0 = time()

    if not os.path.exists(CACHE_FOLDER):
        os.makedirs(CACHE_FOLDER)

    oscars = Oscars(DATA_FOLDER + 'oscars/', CACHE_FOLDER)
    oscars.preprocess()
    lbls = oscars.get_labels()

    imdb = Imdb(CACHE_FOLDER)
    imdb_df = imdb.preprocess()

    pv = PageViews(CACHE_FOLDER + 'viewsperday.csv', imdb_df)
    pv_df = pv.preprocess()

    rev = ImdbReviews(CACHE_FOLDER + 'reviews/', imdb_df)
    rev_df = rev.preprocess(weighted=False)

    data = pd.DataFrame.merge(lbls, imdb_df, on='id')
    data = pd.DataFrame.merge(data, pv_df, on='id')
    data = pd.DataFrame.merge(data, rev_df, on='id')
    logger.info('data.shape = (%s, %s)' % data.shape)

    out_file = CACHE_FOLDER + 'dataset.p'
    with open(out_file, 'wb') as fo:
        cPickle.dump(data, fo)
    logger.info('Saved: %s' % out_file)

    tf = (time() - t0) / 60
    logger.info('--- %0.3f minutes ---' % tf)


if __name__ == '__main__':
    main()

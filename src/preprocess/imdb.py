""" Process OMDB-extracted data  """

from __future__ import division
from collections import defaultdict, namedtuple
from datetime import datetime
import json
import logging
import os

from bs4 import BeautifulSoup
from bs4.element import NavigableString
import pandas as pd
from textblob import TextBlob
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

ReviewBase = namedtuple('ReviewBase', ['id', 'title', 'date', 'votes', 'total',
                                       'text'])

MOVIE_IDS = json.load(open('movie_ids.json', 'rb'))
CEREMONIES = json.load(open('../data/ceremonies.json', 'rb'))


class Review(ReviewBase):
    def sentiment(self):
        tb = TextBlob(self.text)
        return tb.sentiment.polarity


class Imdb(object):
    def __init__(self, cache_folder, select=True):
        self.cache_folder = cache_folder
        self.select = select

    def preprocess(self):
        path = self.cache_folder + 'imdb.csv'
        with open(path, 'rb') as fi:
            data = pd.read_csv(fi)

        imdbVotes = data.ix[:, 'imdbVotes']
        imdbVotes = imdbVotes.apply(lambda x: int(x.replace(',', '')))
        data.ix[:, 'imdbVotes'] = imdbVotes
        return data


class ImdbReviews(object):

    title_to_id = {v: k for k, v in MOVIE_IDS.items()}
    title_to_id["Precious"] = u"472"

    def __init__(self, cache_folder, imdb_df):
        self.cache_folder = cache_folder
        self.imdb_df = imdb_df

    def preprocess(self, weighted=True):
        data = defaultdict(lambda: defaultdict(list))
        feats = defaultdict(list)
        for root, dirs, fns in os.walk(self.cache_folder):
            logger.info('Processing: %s' % root)
            for fn in tqdm(fns):
                if os.path.splitext(fn)[1] != '.html':
                    continue
                for id, r in self.get_reviews(os.path.join(root, fn)):
                    data[id]['sentiment'].append(r.sentiment())
                    data[id]['support'].append(r.votes + 1)
                    data[id]['total'].append(r.total + 1)
        for id, d in data.items():
            feats['id'].append(int(id))
            if weighted:
                numerator = 0
                denominator = sum(d['total'])
                logger.info('movie id: %s  sums: (%d, %d): '
                            % (id, sum(d['support']), denominator))
                for support, sentiment in zip(d['support'], d['sentiment']):
                    numerator += support * sentiment
                score = numerator / denominator
            else:
                score = sum(d['sentiment']) / len(d['sentiment'])
            feats['review_sentiment'].append(score)
        feats = pd.DataFrame(feats)
        return feats

    def get_reviews(self, path):
        logger.debug('Parsing: %s' % path)
        soup = BeautifulSoup(open(path, 'rb'), 'lxml')
        movie = soup.find(id='tn15title').h1.a.text
        id = self.title_to_id[movie]
        tn15 = soup.find(id='tn15content')

        release_date = self.imdb_df[self.imdb_df['id'] == int(id)]['Released']
        release_date = release_date.values[0]
        release_date = datetime.strptime(release_date, "%d %b %Y").date()
        yr = release_date.year
        if yr == 2016:
            yr = 2015
        cer_date = CEREMONIES[str(yr)]
        cer_date = datetime.strptime(cer_date, "%d-%b-%y").date()

        for c in tn15.children:
            if isinstance(c, NavigableString):
                continue
            if c.name == 'div' and c.small:
                title = c.h2.text
                small_tags = c.find_all('small')
                s1 = small_tags[0]
                if s1.find_next().find_next().find_next().name == 'img':
                    header = c.small.text.split()
                    votes, total = int(header[0]), int(header[3])
                else:
                    votes, total = 0, 0
                date = small_tags[-1].text
                date = datetime.strptime(date, '%d %B %Y').date()
                if date >= cer_date:
                    continue
                body = c.next_sibling.next_sibling.text
                r = Review(id, title, date, votes, total, body)
                yield id, r

""" Feature extraction for pageview data """

from datetime import datetime, timedelta
import json
import logging

import numpy as np
import pandas as pd
from tqdm import tqdm

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

CEREMONIES = '../data/ceremonies.json'
WIKI_TITLES = "../src/wiki_titles.json"


class PageViews(object):
    def __init__(self, file_path, imdb_df):
        self.imdb_df = imdb_df
        self.cer = json.load(open(CEREMONIES, 'rb'))

        wt = json.load(open(WIKI_TITLES, 'rb'))
        title_to_id = {v: k for k, v in wt.items()}
        title_to_id[u"The_King's_Speech"] = u"480"
        title_to_id[u'Les_Mis\xc3\xa9rables_(2012_film)'.encode('latin-1')] = u'499'
        title_to_id[u"Winter's_Bone"] = u"485"

        logger.info('Loading: %s' % file_path)
        data = pd.read_csv(open(file_path, 'rb'))

        ids = pd.Series(np.zeros(len(data)), index=data.index)
        for idx, row in tqdm(data.iterrows(), total=len(data)):
            ids[idx] = int(title_to_id[row['title']])
        data['id'] = ids
        self.data = data

    @staticmethod
    def convert_dates(x):
        try:
            return datetime.strptime(str(x), "%Y%m%d").date()
        except ValueError:
            s = str(x)
            y, m, d = s[:4], s[4:6], s[6:]
            logger.debug('Caught ValueError: %s-%s-%s  Trying: %s-%s-%s'
                        % (y, m, d, y, m, int(d)-1))
            d = str(int(d) - 1)
            return PageViews.convert_dates(y + m + d)

    def _extract_feats(self, id, release_date):
        df = self.data[self.data['id'] == id]
        dates = df['date']
        dates = dates.apply(self.convert_dates)
        df['date'] = dates
        release_date = datetime.strptime(release_date, "%d %b %Y").date()

        start = release_date - timedelta(days=30)
        end = release_date
        matching = df[(df['date'] > start) & (df['date'] < end)]
        pv_release_1m = sum(matching['views'])

        yr = release_date.year
        if yr == 2016:
            yr = 2015
        start = self.cer[str(yr)]
        start = datetime.strptime(start, "%d-%b-%y").date()
        end = start + timedelta(days=30)
        matching = df[(df['date'] > start) & (df['date'] < end)]
        pv_oscar_1m = sum(matching['views'])

        f = {'id': id, 'pv-release-1m': pv_release_1m,
             'pv-oscar-1m': pv_oscar_1m}
        return f

    def preprocess(self):
        feats = {'id': [], 'pv-release-1m': [], 'pv-oscar-1m': []}
        for idx, row in self.imdb_df.iterrows():
            id = row['id']
            release_date = row['Released']
            f = (self._extract_feats(id, release_date))
            for k, v in f.items():
                feats[k].append(v)
        feats = pd.DataFrame(feats)
        return feats

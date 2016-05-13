""" Parse Oscar Nomination and Winners Semi-Raw File"""

import csv
import logging

import pandas as pd

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

MIN_YEAR = 2008
RAW_FILENAME = 'nominees.txt'


class Oscars(object):

    ID = 0

    def __init__(self, raw_folder, cache_folder):
        self.raw_folder = raw_folder
        self.cache_folder = cache_folder

    @staticmethod
    def _parse_year(lines):
        year = lines[0].split()[0]
        award = lines[1]
        winner = False
        rows = []
        for i in range(2, len(lines)):
            line = lines[i]
            if line == '*':
                winner = True
                continue
            title, notes = (x.strip() for x in line.split('--'))
            Oscars.ID += 1
            row = (Oscars.ID, year, award, winner, title, notes)
            rows.append(row)
            winner = False
        return rows

    def preprocess(self):
        in_path = self.raw_folder + RAW_FILENAME
        out_path = self.cache_folder + 'oscars.csv'
        fi = open(in_path, 'rb')
        fo = open(out_path, 'wb')
        logger.info('Parsing: %s' % in_path)
        wr = csv.writer(fo)
        wr.writerow(['id', 'year', 'award', 'winner', 'title', 'notes'])
        year_lines = []
        for line in fi:
            line = line.strip()
            if line == '':
                new_rows = self._parse_year(year_lines)
                for r in new_rows:
                    wr.writerow(r)
                year_lines = []
            else:
                year_lines.append(line)
        else:
            new_rows = self._parse_year(year_lines)
            for r in new_rows:
                wr.writerow(r)
        fo.close()
        fi.close()
        logger.info('Saved: %s' % out_path)
        return out_path

    def get_labels(self, path=None):
        if not path:
            path = self.cache_folder + 'oscars.csv'
        with open(path, 'rb') as fi:
            data = pd.read_csv(fi)
        filtered = {'id': [], 'winner': []}
        for idx, row in data.iterrows():
            try:
                year = int(row['year'])
            except ValueError:
                logger.debug('Skipped: %s' % row['year'])
                continue
            if year >= MIN_YEAR:
                label = 1 if row['winner'] else 0
                filtered['id'].append(row['id'])
                filtered['winner'].append(label)
        filtered = pd.DataFrame(filtered)
        return filtered

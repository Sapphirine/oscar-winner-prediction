import cPickle
import json
import logging
import os

import numpy as np
import pandas as pd
from sklearn.metrics import classification_report
from sklearn.preprocessing import scale

from models import Adaboost, LogisticReg, SVM

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__ if __name__ != '__main__' else [])

COLUMNS = ('winner', 'Metascore', 'imdbRating', 'imdbVotes',
           'pv-release-1m', 'pv-oscar-1m', 'review_sentiment')
DATA_CACHE = '../cache/dataset.p'
OUTPUT_FOLDER = '../out/imdb/unweighted/'


CONFIG = [{'name': 'logit',
           'clf': LogisticReg(),
           'params': {'C': [0.001, 0.01, 1, 100, 1000, 10000],
                      'class_weight': ['balanced'],
                      'n_jobs': 1,
                      'verbose': True}},
          {'name': 'svm-rbf',
           'clf': SVM(),
           'params': {'C': [0.001, 0.01, 1, 100, 1000, 10000],
                      'kernel': 'rbf',
                      'gamma': 'auto',
                      'class_weight': 'balanced',
                      'degree': [2, 3],
                      'verbose': True,
                      'max_iter': -1}},
          {'name': 'svm-linear',
           'clf': SVM(),
           'params': {'C': [0.001, 0.01, 1, 100, 1000, 10000],
                      'kernel': 'linear',
                      'class_weight': 'balanced',
                      'verbose': True,
                      'max_iter': -1}},
          {'name': 'svm-poly',
           'clf': SVM(),
           'params': {'C': [0.001, 0.01, 1, 100, 1000, 10000],
                      'kernel': 'poly',
                      'gamma': 'auto',
                      'class_weight': 'balanced',
                      'degree': [2, 3, 4],
                      'verbose': True,
                      'max_iter': -1}},
          {'name': 'adaboost',
           'clf': Adaboost(),
           'params': {'max_depth': [1, 3],
                      'n_estimators': [1, 2, 4, 8, 16, 32],
                      'learning_rate': 1.0}}]


def run_predictions(clf, X, y, show_plot=False, **params):
    best_params = clf.tune(X, y, **params)
    names = ['lose', 'win']
    report = classification_report(y, clf.predict(X), target_names=names)
    logger.info('Training set classification report\n\n%s\n' % report)
    roc_result = clf.mean_roc(X, y, show_plot, **best_params)
    return roc_result


def save_roc(roc_result, path):
    folder = os.path.split(path)[0]
    if not os.path.exists(folder):
        os.makedirs(folder)
    with open(path, 'wb') as fo:
        output = roc_result._asdict()
        for k, v in output.items():
            if isinstance(v, np.ndarray):
                output[k] = v.tolist()
        json.dump(output, fo)
        fo.write('\n')
    logger.info('Saved: %s' % path)


def driver(name, clf, params, X, y):
    roc_result = run_predictions(clf, X, y, **params)
    logger.info('%s roc auc: %f'
                % (name, roc_result.mean_auc))
    path = OUTPUT_FOLDER + name + '.json'
    save_roc(roc_result, path)


def main():
    if not os.path.exists(DATA_CACHE):
        logger.error('No data cached: %s' % DATA_CACHE)

    with open(DATA_CACHE, 'rb') as fi:
        data = cPickle.load(fi)
    assert isinstance(data, pd.DataFrame)

    data = data.ix[:, COLUMNS]
    y = data.ix[:, 'winner']
    X = data.drop('winner', 1)
    X = scale(X)
    X = pd.DataFrame(X)

    for setting in CONFIG:
        name = setting['name']
        clf = setting['clf']
        params = setting['params']
        driver(name, clf, params, X, y)


if __name__ == '__main__':
    main()

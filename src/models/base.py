from collections import namedtuple
import logging
from time import time

import matplotlib.pyplot as plt
import numpy as np
from scipy import interp
from sklearn.cross_validation import StratifiedKFold
from sklearn.grid_search import GridSearchCV
from sklearn.metrics import roc_curve, auc

logger = logging.getLogger(__name__)

MeanROC = namedtuple('MeanROC', ['mean_fpr', 'mean_tpr', 'mean_auc'])


def timed(f):
    def timer(*args, **kwargs):
        t0 = time()
        result = f(*args, **kwargs)
        tf = (time() - t0) / 60
        logger.info("--- %0.3f minutes ---" % tf)
        return result
    return timer


class BaseClf(object):
    def __init__(self):
        self.clf = None
        self.mdl = None

    @staticmethod
    def cv(y, n_folds=3, shuffle=True, random_state=123):
        skf = StratifiedKFold(y, n_folds=n_folds, shuffle=shuffle,
                              random_state=random_state)
        return skf

    @staticmethod
    def prep_param_grid(param_grid):
        for grp in param_grid:
            for k, v in grp.items():
                if not isinstance(v, list):
                    grp[k] = [v]
        return param_grid

    @timed
    def tune(self, X, y, param_grid, n_jobs=1, verbose=0):
        logger.info("Grid searching (10-fold cv)")
        self.clf = GridSearchCV(self.mdl(), param_grid=param_grid,
                                n_jobs=n_jobs, cv=self.cv(y), verbose=verbose,
                                scoring='roc_auc')
        self.clf.fit(X, y)
        self.report_cv_scores()
        return self.clf.best_params_

    def predict(self, X):
        return self.clf.predict(X)

    def predict_proba(self, X):
        return self.clf.predict_proba(X)

    def report_cv_scores(self):
        logger.info("--- CV Scores ---")
        for params, mean_cv_score, cv_scores in self.clf.grid_scores_:
            logger.info("cv score: %0.5f (+/-%0.05f) for %r"
                        % (mean_cv_score, cv_scores.std() * 2, params))

        logger.info("--- Summary ---")
        logger.info("Best parameters: %s" % self.clf.best_params_)
        logger.info("Best cv score: %0.5f" % self.clf.best_score_)

    def mean_roc(self, X, y, show_plot=False, **best_params_):
        """
        Based on tutorial at
        http://scikit-learn.org/stable/auto_examples/model_selection/plot_roc_crossval.html
        """
        clf = self.mdl(**best_params_)
        cv = self.cv(y)
        mean_tpr = 0.0
        mean_fpr = np.linspace(0, 1, y.shape[0])

        for i, (train, test) in enumerate(cv):
            probas_ = clf.fit(X.ix[train], y.ix[train]).predict_proba(X.ix[test])
            fpr, tpr, thresholds = roc_curve(y.ix[test], probas_[:, 1])
            mean_tpr += interp(mean_fpr, fpr, tpr)
            mean_tpr[0] = 0.0
            roc_auc = auc(fpr, tpr)
            plt.plot(fpr, tpr, lw=1, label='ROC fold %d (area = %0.2f)'
                                           % (i, roc_auc))

        plt.plot([0, 1], [0, 1], '--', color=(0.6, 0.6, 0.6), label='Luck')

        mean_tpr /= len(cv)
        mean_tpr[-1] = 1.0
        mean_auc = auc(mean_fpr, mean_tpr)
        plt.plot(mean_fpr, mean_tpr, 'k--',
                 label='Mean ROC (area = %0.2f)' % mean_auc, lw=2)

        plt.xlim([-0.05, 1.05])
        plt.ylim([-0.05, 1.05])
        plt.xlabel('False Positive Rate')
        plt.ylabel('True Positive Rate')
        plt.title(self.__class__.__name__ +
                  ': Receiver operating characteristic')
        plt.legend(loc="lower right")
        if show_plot:
            plt.show()
        return MeanROC(mean_fpr, mean_tpr, mean_auc)

    def __str__(self):
        return "<%s %s>" % (self.__class__.__name__, self.__dict__)

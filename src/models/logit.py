import logging

from sklearn.linear_model import LogisticRegression

from base import BaseClf

logger = logging.getLogger(__name__)


class LogisticReg(BaseClf):
    def __init__(self):
        super(LogisticReg, self).__init__()
        self.mdl = LogisticRegression

    def tune(self, X, y, C=1.0, penalty='l1', class_weight=None,
             n_jobs=1, verbose=0):
        param_grid = [{'C': C,
                       'penalty': penalty,
                       'class_weight': class_weight}]
        param_grid = self.prep_param_grid(param_grid)
        return super(LogisticReg, self).tune(X, y, param_grid,
                                             n_jobs=n_jobs, verbose=verbose)

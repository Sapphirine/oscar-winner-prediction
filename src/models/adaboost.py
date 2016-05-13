import logging

from sklearn.ensemble import AdaBoostClassifier
from sklearn.tree import DecisionTreeClassifier

from base import BaseClf

logger = logging.getLogger(__name__)


class Adaboost(BaseClf):
    def __init__(self):
        super(Adaboost, self).__init__()
        self.mdl = AdaBoostClassifier

    def tune(self, X, y, max_depth=1, n_estimators=50, learning_rate=1.0,
             n_jobs=1, verbose=0):
        if not isinstance(max_depth, list):
            max_depth = [max_depth]
        base_estimators = []
        for md in max_depth:
            base_estimator = DecisionTreeClassifier(max_depth=md)
            base_estimators.append(base_estimator)
        param_grid = [{'base_estimator': base_estimators,
                       'n_estimators': n_estimators,
                       'learning_rate': learning_rate,
                       'random_state': 123}]
        param_grid = self.prep_param_grid(param_grid)
        return super(Adaboost, self).tune(X, y, param_grid,
                                          n_jobs=n_jobs, verbose=verbose)

import logging

from sklearn.svm import SVC

from base import BaseClf

logger = logging.getLogger(__name__)


class SVM(BaseClf):
    def __init__(self):
        super(SVM, self).__init__()
        self.mdl = SVC

    def tune(self, X, y, C=1.0, kernel='rbf', gamma='auto', class_weight=None,
             degree=3, max_iter=-1, n_jobs=1, verbose=0):
        param_grid = [{'C': C,
                       'kernel': kernel,
                       'gamma': gamma,
                       'class_weight': class_weight,
                       'degree': degree,
                       'max_iter': max_iter,
                       'probability': True,
                       'random_state': 123}]
        param_grid = self.prep_param_grid(param_grid)
        return super(SVM, self).tune(X, y, param_grid,
                                     n_jobs=n_jobs, verbose=verbose)

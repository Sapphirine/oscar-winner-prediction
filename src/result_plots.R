library(dplyr)
library(jsonlite)
library(ggplot2)
# library(ggthemes)

setwd('~/Documents/CS/EECS-E6895-ABDA/project/src/')

data_paths = data.frame(set=c(rep('IMDB (weighted)', 5),
                              rep('IMDB (uniform)', 5)),
                        model=rep(c('adaboost', 
                                    'logit', 
                                    'svm-linear', 
                                    'svm-poly', 
                                    'svm-rbf'), 2), 
                        path=c('../out/imdb/adaboost.json', 
                               '../out/imdb/logit.json',
                               '../out/imdb/svm-linear.json',
                               '../out/imdb/svm-poly.json',
                               '../out/imdb/svm-rbf.json',
                               '../out/imdb/unweighted/adaboost.json', 
                               '../out/imdb/unweighted/logit.json',
                               '../out/imdb/unweighted/svm-linear.json',
                               '../out/imdb/unweighted/svm-poly.json',
                               '../out/imdb/unweighted/svm-rbf.json'),
                        stringsAsFactors=FALSE)

data_paths$json <- apply(data_paths, 1, function(z) readLines(z['path']))

makeAUC <- function(row) {
  d <- fromJSON(row['json'])
  df <- data.frame(set=row['set'], model=row['model'], auc=d[[3]],
                   stringsAsFactors=FALSE)
  df
}

makeCURVES <- function(row) {
  d <- fromJSON(row['json'])
  df <- data.frame(set=rep(row['set']), model=rep(row['model']), 
                   mean_fpr=d[[1]], mean_tpr=d[[2]],
                   stringsAsFactors=FALSE)
  row.names(df) <- NULL
  df
}

AUC <- do.call('rbind', apply(data_paths, 1, makeAUC))
CURVES <- do.call('rbind', apply(data_paths, 1, makeCURVES))

CURVES <- filter(CURVES, set != 'Rotten Tomatoes')

p1 <- ggplot(CURVES, aes(x=mean_fpr, y=mean_tpr, color=model)) +
        facet_wrap(~set) +
        geom_line(size=2, alpha=0.7) +
        geom_abline(slope=1, intercept=0, linetype=2) +
        scale_x_continuous(limits=c(0, 1), expand=c(0.01, 0.01)) +
        scale_y_continuous(limits=c(0, 1), expand=c(0.01, 0.01)) +
        labs(title="", x="False Postive Fraction", y="True Postive Fraction") +
        theme(axis.text.x = element_text(angle=90, vjust=0.2))


p2 <- ggplot(CURVES, aes(x=mean_fpr, y=mean_tpr, color=model, linetype=set)) +
        geom_line(size=1.5, alpha=0.8) +
        geom_abline(slope=1, intercept=0, linetype=2) +
        scale_x_continuous(limits=c(0, 1), expand=c(0.01, 0.01)) +
        scale_y_continuous(limits=c(0, 1), expand=c(0.01, 0.01)) +
        labs(title="ROC Curves", x="False Postive Fraction", y="True Postive Fraction")

pdf('../out/roc.pdf', 8, 5)
p1
dev.off()


AUC


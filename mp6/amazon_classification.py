from pyspark import SparkContext, SparkConf
import argparse
import csv
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.regression import LabeledPoint
from pyspark.mllib.classification import NaiveBayes
from pyspark.mllib.evaluation import MulticlassMetrics


def loadcsv(f):
    x = csv.reader([f])
    x = list(x)[0]
    if x!=None and x[6]!='Score' and x[6]!=None:
        if float(x[6]) < 3:
            return (0, x[9])
        else:
            return (1, x[9])
    return None



def amazon_classification(sc, filename):
    '''
    Args:
        sc: The Spark Context
        filename: Filename of the Amazon reviews file to use, where each line represents a review
    '''
    # Load in reviews
    reviews = sc.textFile(filename).sample(False, 0.001)

    # Parse to csv
    csv_loads = reviews.map(loadcsv)

    #
    labeled_data = (csv_loads.filter(lambda x: x != None).mapValues(lambda x: x.split()))

    labels = labeled_data.keys()

    tf = HashingTF().transform(labeled_data.map(lambda x:x[1]))
    idf = IDF(minDocFreq=7).fit(tf)
    tfidf = idf.transform(tf)
    labeled_points = (labels.zip(tfidf)
                         .map(lambda x: LabeledPoint(float(x[0]), x[1])))

    training, test = labeled_points.randomSplit([0.6, 0.4])

    model = NaiveBayes.train(training)

    # Use our model to predict
    train_preds = (training.map(lambda x: x.label)
                           .zip(model.predict(training.map(lambda x: x.features))))
    test_preds = (test.map(lambda x: x.label)
                      .zip(model.predict(test.map(lambda x: x.features))))

    # Ask PySpark for some metrics on how our model predictions performed
    trained_metrics = MulticlassMetrics(train_preds.map(lambda x: (x[0], float(x[1]))))
    test_metrics = MulticlassMetrics(test_preds.map(lambda x: (x[0], float(x[1]))))
    ojbk = open('./xxx.txt','w+')
    ojbk.write(str(trained_metrics.confusionMatrix().toArray()) + '\n')
    ojbk.write(str(trained_metrics.precision()) + '\n')
    ojbk.write(str(test_metrics.confusionMatrix().toArray()) + '\n')
    objk.write(str(test_metrics.precision()) + '\n')

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("amazon_classification")
    sc = SparkContext(conf=conf)

    amazon_classification(sc, args.input)

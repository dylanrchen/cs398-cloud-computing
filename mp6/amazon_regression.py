from pyspark import SparkContext, SparkConf
import argparse
import csv 
from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark.mllib.evaluation import RegressionMetrics
from pyspark.mllib.feature import HashingTF, IDF


def f(x):
    print (x)
    
def loadcsv(x):
    x = csv.reader(x)
    x = list(x)[0]
    if x[0] =='Id' or len(x)<10:
        return None
    if x[4]!=None and x[5] !=None and x[9] !=None and x[5]!=0:
        return (float(x[4])/float(x[5]),x[9])

def amazon_regression(sc, filename):
    '''
    Args:
        sc: The Spark Context
        filename: Filename of the Amazon reviews file to use, where each line represents a review    
    '''

    # YOUR CODE HERE
    reviews = sc.textFile(filename).sample(False,0.0001)
    reviews = reviews.map(lambda x:loadcsv(x))
    reviews = reviews.filter(lambda x: x != None)
    labels = reviews.map(lambda x:x[0])
   # reviews = reviews.map(lambda x: (float(x[0]), x[1])).mapValues(lambda x:x.split())
    reviews = (reviews .map(lambda x: (float(x[0]), x[1]))
                             .mapValues(lambda x: x.split()))    
# Feed HashingTF the array of words
    tf = HashingTF().transform(reviews.map(lambda x:x[1]))
    # Pipe term frequencies into the IDF
    idf = IDF(minDocFreq=5).fit(tf)
    # Transform the IDF into a TF-IDF
    tfidf = idf.transform(tf)
    parsedData = (labels.zip(tfidf)
                         .map(lambda x: LabeledPoint((x[0]), x[1])))
    training, test = parsedData.randomSplit([0.5, 0.5])
    # Build the model
    model = LinearRegressionWithSGD.train(training, iterations=10000, step=0.000000001)

    # Evaluate the model on training data
    valuesAndPreds = (training.map(lambda x: x.label)
                              .zip(model.predict(training.map(lambda x: x.features))))

    # Save and load mode
    vap = (test.map(lambda x: x.label)
               .zip(model.predict(test.map(lambda x: x.features))))
    trained_metrics = RegressionMetrics(valuesAndPreds.mapValues(float))
    train_rootMeanSquaredError = trained_metrics.rootMeanSquaredError
    train_explainedVariance = trained_metrics.explainedVariance
    test_metrics = RegressionMetrics(vap.mapValues(float))
    test_rootMeanSquaredError = test_metrics.rootMeanSquaredError
    test_explainedVariance = test_metrics.explainedVariance
#    return reviews

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("amazon_regression")
    sc = SparkContext(conf=conf)

    results = amazon_regression(sc, args.input)
 #   results.saveAsTextFile('./x')

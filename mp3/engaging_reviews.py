from pyspark import SparkContext, SparkConf
import argparse
import json
def f(a,b):
    print(a)
    print (b)
    if a[0]>b[0]:
        return a
    else:
        if (a[1]>b[1] and a[0] == b[0]):
            return a
        return b

def find_engaging_reviews(sc, reviews_filename):
    textfile = sc.textFile(reviews_filename)
    j = textfile.map(lambda x:x.split('\n')).map(lambda x:
    (json.loads(x[0])['business_id'],(json.loads(x[0])['funny']+json.loads(x[0])\
    ['cool']+json.loads(x[0])['useful'],json.loads(x[0])['review_id'])))
    
    k = j.reduceByKey(lambda a,b: f(a,b)).map(lambda x:(x[0],x[1][1]))
    # j = j.reduceByKey(lambda a,b: f(a,b))
    return k
    # j = j.(lambda x,y:)
    
    '''
    Args:
        sc: The Spark Context
        reviews_filename: Filename of the Yelp reviews JSON file to use, where each line represents a review
    Returns:
        An RDD of tuples in the following format:
            (BUSINESS_ID, REVIEW_ID)
            - BUSINESS_ID: The business being referenced
            - REVIEW_ID: The ID of the review with the largest sum of "useful", "funny", and "cool" responses
                for the given business
    '''

    # YOUR CODE HERE

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp review data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("engaging_reviews")
    sc = SparkContext(conf=conf)

    results = find_engaging_reviews(sc, args.input)
    results.saveAsTextFile(args.output)


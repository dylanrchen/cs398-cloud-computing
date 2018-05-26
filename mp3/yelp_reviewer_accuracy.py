from pyspark import SparkContext, SparkConf
import argparse
import json
def f(a):
    # print(a)
    return a

def find_user_review_accuracy(sc, reviews_filename):
    text = sc.textFile(reviews_filename)
    line = text.map(lambda x :x.split('\n'))
    j = line.map(lambda x: (json.loads(x[0])['business_id'],(json.loads(x[0])['stars'],1))).reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])).map(lambda x:(x[0],x[1][0]/x[1][1]))
    # now we have the average rating of every restaurant
    #  next step, join by bussiness and see
    f = line.map(lambda x:(json.loads(x[0])['business_id'],(json.loads(x[0])['user_id'],json.loads(x[0])['stars'])))
    k = f.join(j).map(lambda x:(x[1][0][0],(x[1][0][1]-x[1][1],1)))\
        .reduceByKey(lambda a,b:(a[0]+b[0],a[1]+b[1])).map(lambda x:(x[0],round(x[1][0]/x[1][1],2)))
    return k
    # f = line.map(lambda x:(json.loads(x[0])['business_id'],(json.loads(x[0])['stars'],1))
    return j
    '''
    Args:
        sc: The Spark Context
        reviews_filename: Filename of the Yelp reviews JSON file to use, where each line represents a review
    Returns:
        An RDD of tuples in the following format:
            (USER_ID, AVERAGE_REVIEW_OFFSET)
            - USER_ID: The ID of the user being referenced
            - AVERAGE_REVIEW_OFFSET: The average difference between a user's review and the average restaraunt rating
    '''
    
    # YOUR CODE HERE

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp review data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("yelp_reviewer_accuracy")
    sc = SparkContext(conf=conf)

    results = find_user_review_accuracy(sc, args.input)
    results.saveAsTextFile(args.output)


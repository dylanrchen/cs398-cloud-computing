from pyspark import SparkContext, SparkConf
import argparse


def yelp_clustering(sc, filename):
    '''
    Args:
        sc: The Spark Context
        filename: Filename of the yelp businesses file to use, where each line represents a business
    '''

    # YOUR CODE HEREHERE

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp business data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("yelp_clustering")
    sc = SparkContext(conf=conf)

    yelp_clustering(sc, args.input)

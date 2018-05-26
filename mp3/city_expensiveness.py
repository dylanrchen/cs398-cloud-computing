from pyspark import SparkContext, SparkConf
import argparse
import json
import re
def f(city):
    # print (city)
    print(city)

price = r'RestaurantsPriceRange2: [0-5]'
PRICE = re.compile(price)
def findprice(arr):
    if arr == None:
        return None
    for v in arr:
        if PRICE.match(v):
            return PRICE.findall(v)[0][-1]
    return None
        


def find_city_expensiveness(sc, business_filename):
    # file = open(business_filename)
    # lines = file.readlines()
    tx = sc.textFile(business_filename)
    j = tx.map(lambda x:x.split('\n')).map(lambda x:(json.loads(x[0])['city']+', '+json.loads(x[0])['state'],\
    (findprice(json.loads(x[0])['attributes']))))
    j = j.filter(lambda x: x[1]!=None).map(lambda x: (x[0],(float(x[1]),1.0))).\
    reduceByKey(lambda a,b :(a[0]+b[0],a[1]+b[1]))\
    .map(lambda x:(x[0],round(x[1][0]/x[1][1],2)))
    
    return j
    # average = textfile.map(lambda line: json.load(line[4],line[5])).map(lambda city:(city))\
    #             .reduceByKey(lambda a,b:a+b)
    # return (average.collect())
    '''
    Args:
        sc: The Spark Context
        business_filename: Filename of the Yelp businesses JSON file to use, where each line represents a business
    Returns:
        An RDD of tuples in the following format:
            (CITY_STATE, AVERAGE_PRICE)
            - CITY_STATE is in the format "CITY, STATE". i.e. "Urbana, IL"
            - AVERAGE_PRICE should be a float rounded to 2 decimal places
    '''

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Yelp business data from')
    parser.add_argument('output', help='File to save RDD to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("city_expensiveness")
    sc = SparkContext(conf=conf)

    results = find_city_expensiveness(sc, args.input)
    results.saveAsTextFile(args.output)


from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse

def setup_table(sc, sqlContext, users_filename, businesses_filename, reviews_filename):
    usertable = sqlContext.read.json(users_filename)
    bustable = sqlContext.read.json(businesses_filename)
    reviewtable = sqlContext.read.json(reviews_filename)
    sqlContext.registerDataFrameAsTable(usertable,'usertable')
    sqlContext.registerDataFrameAsTable(bustable,'bustable')
    sqlContext.registerDataFrameAsTable(reviewtable,'reviewtable')
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        users_filename: Filename of the Yelp users file to use, where each line represents a user
        businesses_filename: Filename of the Yelp checkins file to use, where each line represents a business
        reviews_filename: Filename of the Yelp reviews file to use, where each line represents a review
    Parse the users/checkins/businesses files and register them as tables in Spark SQL in this function
    '''

def query_1(sc, sqlContext):
    ut = sqlContext.sql('select user_id as iddd from usertable '
    'where yelping_since LIKE "2012%"')
    sqlContext.registerDataFrameAsTable(ut,'newuser')
    newtable = sqlContext.sql('select funny from reviewtable INNER JOIN newuser ON reviewtable.user_id = newuser.iddd')
    sqlContext.registerDataFrameAsTable(newtable,'newtable')
    newtable = sqlContext.sql('select MAX(funny) as m from newtable')
    return newtable.take(1)[0]['m']
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the maximum number of "funny" ratings left on a review created by someone who started "yelping" in 2012
    '''

def query_2(sc, sqlContext):
    champaign = sqlContext.sql('select city,business_id from bustable where city = "Champaign"')
    sqlContext.registerDataFrameAsTable(champaign,"cham")
    userstar= sqlContext.sql('select user_id,business_id from reviewtable where stars=1')
    sqlContext.registerDataFrameAsTable(userstar,'userstar')
    userCham = sqlContext.sql('select user_id from userstar inner join cham on cham.business_id = userstar.business_id')
    sqlContext.registerDataFrameAsTable(userCham,'userCham')
    #print (userCham.collect())
    user250 = sqlContext.sql('select user_id as us from usertable where review_count>250 ')
    # print (user250.collect())
    sqlContext.registerDataFrameAsTable(user250,'fil')
    inner2 = sqlContext.sql('select distinct user_id from userCham inner join fil on fil.us = userCham.user_id')
    #print (inner2)
    inner2 =inner2.collect()
    ret = [inner2[i]['user_id'] for i in range(len(inner2))]
    return ret
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of strings: the user ids of anyone who has left a 1-star review, has created more than 250 reviews,
            and has left a review at a business in Champaign, IL
    '''

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('users', help='File to load Yelp user data from')
    parser.add_argument('businesses', help='File to load Yelp business data from')
    parser.add_argument('reviews', help='File to load Yelp review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("jaunting_with_joins")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    setup_table(sc, sqlContext, args.users, args.businesses, args.reviews)

    result_1 = query_1(sc, sqlContext)
    result_2 = query_2(sc, sqlContext)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    print("Query 1: {}".format(result_1))
    print("Query 2: {}".format(result_2))
    print("-" * 30)

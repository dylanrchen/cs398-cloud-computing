from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
from pyspark.sql.types import *
def setup_table(sc, sqlContext, reviews_filename):
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review    
    Parse the reviews file and register it as a table in Spark SQL in this function
    '''
    myschema = StructType([StructField("ID",IntegerType(),True),
                        StructField("ProductId",StringType(),True),
                        StructField("UserId",StringType(),True),
                        StructField("ProfileName",StringType(),True),
                        StructField("HelpfulnessNumerator",IntegerType(),True),
                        StructField("HelpfulnessDenominator",IntegerType(),True),
                        StructField("Score",IntegerType(),True),
                        StructField("Time",IntegerType(),True),
                        StructField("Summary",StringType(),True),
                        StructField("Text",StringType(),True)]                       
                        )
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load(reviews_filename,schema = myschema)
    # df.write.format('com.databricks.spark.csv').options(header= 'true').save('./4')
    # df = sqlContext.read.csv(reviews_filename)
    # df.write.csv('./4')
    sqlContext.registerDataFrameAsTable(df,"table1")

def query_1(sc, sqlContext):
    table1 = sqlContext.table('table1')
    df =table1.filter(table1['ID']==22010).collect()[0]['Text']
    return df
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An string: the review text of the review with id `22010`
    '''

def query_2(sc, sqlContext):
    table1 =sqlContext.table('table1')
    df = table1.filter(table1['ProductId']=='B000E5C1YE')
    df = df.filter(df['Score']==5).groupBy('Score').count().collect()
    return df[0]['count']
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the number of 5-star ratings the product `B000E5C1YE` has
    '''

def query_3(sc, sqlContext):
    table1 = sqlContext.table('table1')
    user = table1.select('UserId').distinct().count()

    return user
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        An int: the number unique (distinct) users that have written reviews
    '''

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("quizzical_queries")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    setup_table(sc, sqlContext, args.input)

    result_1 = query_1(sc, sqlContext)
    result_2 = query_2(sc, sqlContext)
    result_3 = query_3(sc, sqlContext)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    print("Query 1: {}".format(result_1))
    print("Query 2: {}".format(result_2))
    print("Query 3: {}".format(result_3))
    print("-" * 30)

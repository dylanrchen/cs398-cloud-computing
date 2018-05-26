from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import argparse
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import count, avg

def setup_table(sc, sqlContext, reviews_filename):
    # text = sc.textFile(reviews_filename)
    # text = text.map(lambda x:x.split(','))
    # # field= [StructField]
    # text = text.map(lambda p:(p[0],p[1].strip()))
    # field = [Struct(format)]
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
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review    
    Parse the reviews file and register it as a table in Spark SQL in this function
    '''

def query_1(sc, sqlContext):
    df = sqlContext.table('table1')
    df = df.groupby('UserId').count()
    rowmax = df.agg({"count":"max"}).collect()[0]
    return rowmax['max(count)']
    ''' 
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
        reviews_filename: Filename of the Amazon reviews file to use, where each line represents a review
    Returns:
        An int: the number of reviews written by the person with the maximum number of reviews written
    '''

def query_2(sc, sqlContext):
    # df = sqlContext.table('table1')
    # df = df.groupby('ProductId','Score')
    df = sqlContext.sql('select ProductId, avg(Score) AS avg,count(*) AS count '
                        'from table1 '
                        'GROUP BY ProductId Having count > 25 ORDER BY avg DESC, count Desc' ).take(25)
    # df = df.groupby('ProductId').agg(avg('Score'),count('*'))
    # f = df.orderBy(['avg(Score)','count(1)'])
    ret  = [df[i]['ProductId'] for i in range(len(df))]
    return ret
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of strings: The product ids of the products with the top 25 highest average
            review scores of the products with more than 25 reviews,
            ordered by average product score, with ties broken by the number of reviews
    '''

def query_3(sc, sqlContext):
    df = sqlContext.sql('select ID,HelpfulnessNumerator / HelpfulnessDenominator as help,'
                        'HelpfulnessDenominator as ff From table1 '
                        'WHERE HelpfulnessDenominator>10 Order BY help Desc,ff Desc').take(25)
    ret = [df[i]['ID'] for i in range(len(df))]
    return ret
    '''
    Args:
        sc: The Spark Context
        sqlContext: The Spark SQL context
    Returns:
        A list of integers: A `Ids` of the reviews with the top 25 highest ratios between `HelpfulnessNumerator`
            and `HelpfulnessDenominator`, which have `HelpfulnessDenominator` greater than 10,
            ordered by that ratio, with ties broken by `HelpfulnessDenominator`.
    '''

if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()
    parser.add_argument('input', help='File to load Amazon review data from')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("aggregation_aggrevation")
    sc = SparkContext(conf=conf)
    sqlContext = SQLContext(sc)

    setup_table(sc, sqlContext, args.input)
    result_3 = query_3(sc, sqlContext)
    result_1 = query_1(sc, sqlContext)
    result_2 = query_2(sc, sqlContext)

    print("-" * 15 + " OUTPUT " + "-" * 15)
    print("Query 1: {}".format(result_1))
    print("Query 2: {}".format(result_2))
    print("Query 3: {}".format(result_3))
    print("-" * 30)

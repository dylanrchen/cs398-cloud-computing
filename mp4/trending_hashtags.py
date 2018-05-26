from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import  json
import re
H = r'#\w+'
HASH = re.compile(H)

def f(x):
    try:
        js = json.loads(x)
        if not js:
            return None
        text = js['text']
        return text
    except:
        return ""

    

windowlength = 60
slide_interval = 10


def find_trending_hashtags(sc, input_dstream):
    j = input_dstream.flatMap(lambda x:HASH.findall(x)).map(lambda x:(x,1))
    k = j.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,windowlength,slide_interval)
    k = k.transform(lambda x:x.context.parallelize(x.top(10, key=lambda i:i[1])))
    k.pprint()
    return k


    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the top 10 trending hashtags, and their usage count
    '''

    # YOUR CODE HERE


if __name__ == '__main__':
    # Get input/output files from user
    parser = argparse.ArgumentParser()

    parser.add_argument('--input_stream', help='Stream URL to pull data from')
    parser.add_argument('--input_stream_port', help='Stream port to pull data from')

    parser.add_argument('output', help='Directory to save DStream results to')
    args = parser.parse_args()

    # Setup Spark
    conf = SparkConf().setAppName("trending_hashtags")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = find_trending_hashtags(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()

from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import json

def f(x):
    try:
        x = json.loads(x)
        username = x['author']
        text = x['text']
        subreddit = x['subreddit']
        if len(text.split()) > 10:
            return (username, (text, 0))
        else:
            return (username, (text, 1))
    except:
        return ("",("",0))


def detect_reddit_bots(sc, input_dstream):
    k = input_dstream.flatMap(lambda x:f(x))
    k = k.reduceByKeyAndWindow(lambda x,y:(x[1]+y[1]),lambda x,y:x[1]-y[1],900,10).filter(lambda x:x[1]>10).map(lambda x:x[0])
    return k
    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the list of all detected bot usernames
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
    conf = SparkConf().setAppName("reddit_bot_detection")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = detect_reddit_bots(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()

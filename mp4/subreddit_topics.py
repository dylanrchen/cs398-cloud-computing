from pyspark import SparkContext, SparkConf
from pyspark.streaming import StreamingContext
import argparse
import sys
import re
import json
stopwords_str = 'i,me,my,myself,we,our,ours,ourselves,you,your,yours,yourself,yourselves,he,him,his,himself,she,her,hers,herself,it,its,itself,they,them,their,theirs,themselves,what,which,who,whom,this,that,these,those,am,is,are,was,were,be,been,being,have,has,had,having,do,does,did,doing,a,an,the,and,but,if,or,because,as,until,while,of,at,by,for,with,about,against,between,into,through,during,before,after,above,below,to,from,up,down,in,out,on,off,over,under,again,further,then,once,here,there,when,where,why,how,all,any,both,each,few,more,most,other,some,such,no,nor,not,only,own,same,so,than,too,very,s,t,can,will,just,don,should,now,d,ll,m,o,re,ve,y,ain,aren,couldn,didn,doesn,hadn,hasn,haven,isn,ma,mightn,mustn,needn,shan,shouldn,wasn,weren,won,wouldn,dont,cant'
stopwords = set(stopwords_str.split(','))

word = r'[A-Za-z]+'
WORD = re.compile(word)
def f(x):
    if not x:
        return ('',[])
    try:
        x = json.loads(x)
        subre = x['subreddit']
        text = x['text']
        words = WORD.findall(text)
        ret = []
        for i in words:
            l = i.lower()
            l = l.replace('\'','')
            if l not in stopwords and l not in ret:
                ret.append(l)
        #         if l in ret.keys():
        #             ret[l]+=1
        #         else:
        #             ret[l] = 1
        # rett = []
        # for key,value in ret.items():
        #     rett.append((key,value))
        return (subre,ret)
    #
    except:
        return ("",[])
        return ("",[])

def com(x,y):
    for i in y:
        if i not in x:
            x.append(i)

def oo(x):
    print (1,x,2)
    return x

def find_subreddit_topics(sc, input_dstream):
    # input_dstream.pprint()
    k = input_dstream.map(lambda x:f(x)).flatMapValues(lambda x:x)
    k = k.map(lambda x:(x,1))
    #return k
    # k.pprint()
    # return k
    # return k
    count = k.reduceByKeyAndWindow(lambda x,y:x+y,lambda x,y:x-y,900,10)
    # count.pprint()
    count = count.map(lambda x:(x[0][0],(x[0][1],x[1])))
    count = count.transform(lambda x:x.context.parallelize(x.top(10, key=lambda i:i[1]))).map(lambda x:(x[0],[x[1][0]]))
    a = count.reduceByKey(lambda x,y:x+y)
    a = a.map(lambda x:(x[0],tuple(x[1])))
    a.pprint()
    return a

    # k = k.reduceByKeyAndWindow(lambda x,y:comb(x,y))
    #return a
    '''
    Args:
        sc: the SparkContext
        input_dstream: The discretized stream (DStream) of input
    Returns:
        A DStream containing the list of common subreddit words
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
    conf = SparkConf().setAppName("subreddit_topics")
    sc = SparkContext(conf=conf)
    ssc = StreamingContext(sc, 10)
    ssc.checkpoint('streaming_checkpoints')

    if args.input_stream and args.input_stream_port:
        dstream = ssc.socketTextStream(args.input_stream,
                                       int(args.input_stream_port))
        results = find_subreddit_topics(sc, dstream)
    else:
        print("Need input_stream and input_stream_port")
        sys.exit(1)

    results.saveAsTextFiles(args.output)
    results.pprint()

    ssc.start()
    ssc.awaitTermination()

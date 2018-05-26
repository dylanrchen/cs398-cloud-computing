from mrjob.job import MRJob
import re

reg = r"[A-Za-z]+"
WORD_REGEX =re.compile(reg)

class BigramCount(MRJob):
    '''
        Input: List of lines containing sentences (possibly many sentences per line)
        Output: A generated list of key/value tuples:
            Key: A bigram separated by a comma (i.e. "the,cat")
            Value: The number of occurences of that bigram (integer)
    '''

    def mapper(self, key, val):
        ret =  WORD_REGEX.findall(val)
        for word1,word2 in zip(ret[:-1],ret[1:]):
            yield (word1,1)


    def reducer(self, key, vals):
        print (key,"ssss")
        total = 0
        # for _ in vals:
        #     total += 1
        # yield key,total


if __name__ == '__main__':
    BigramCount.SORT_VALUES = True
    BigramCount.run()
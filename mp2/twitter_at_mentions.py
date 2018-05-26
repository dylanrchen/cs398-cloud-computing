from mrjob.job import MRJob
import re
reg = r'@[a-zA-z_0-9]{3,16}'
REG = re.compile(reg)

class TwitterAtMentions(MRJob):
    '''
        Input: List of lines containing tab-separated tweets with following format:
            POST_DATETIME <tab> TWITTER_USER_URL <tab> TWEET_TEXT

        Output: A generated list of key/value tuples:
            Key: Twitter user handle (including '@' prefix)
            Value: Number of @-mentions received
    '''

    def mapper(self, key, val):
        matches = REG.findall(val)
        # print (matches)
        ret = []
        for v in matches:
            if len(v)<=16 or (v[16]!='_' and (not v[16].isalnum) and v[16]!='_'):
                if v not in ret:
                    ret.append(v)
        for k in ret:
            yield (k,1)

    def reducer(self, key, vals):
        i = 0
        # print (key)
        for v in vals:
            i+=1
        yield (key,i)


if __name__ == '__main__':
    TwitterAtMentions.run()
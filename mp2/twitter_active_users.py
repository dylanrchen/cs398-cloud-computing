from mrjob.job import MRJob
import re

twitter = r'http://twitter.com/[0-9A-Za-z_]*'
TWI = re.compile(twitter)

DAY = re.compile('^[0-9]{4}-[0-9]{2}-[0-9]{2}')
class TwitterActiveUsers(MRJob):
    '''
        Input: List of lines containing tab-separated tweets with following format:
            POST_DATETIME <tab> TWITTER_USER_URL <tab> TWEET_TEXT

        Output: A generated list of key/value tuples:
            Key: Day in `YYYY-MM-DD` format
            Value: Twitter user handle of the user with the most tweets on this day
                (including '@' prefix)
    '''

    def mapper(self, key, val):
        twi = val.split()[2]
        user = '@'+twi[19:]
        time = DAY.findall(val)
        yield (time[0],user)

    def reducer(self, key, vals):
        prev = None
        counter =0
        max =0
        currmax =None
        for k in vals:
           
            if prev==None:
                currmax = k
                prev=k
                max =1
                counter =1
            else:
                print (currmax,k)
                if (prev!=k):
                    counter =1
                    prev = k
                else:
                    counter+=1
                    if counter==max:
                        if k<currmax:
                            currmax = k
                    if counter>max:
                        print (counter,max,"fff")
                        currmax = k
                        max =counter
        yield(key,currmax)

if __name__ == '__main__':
    TwitterActiveUsers.SORT_VALUES = True
    TwitterActiveUsers.run()
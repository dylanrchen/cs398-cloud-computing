from mrjob.job import MRJob
import re
from mrjob.step import MRStep


reg = r"[0-9]+"
REG = re.compile(reg)
class TwitterFollowers(MRJob):
    '''
        Input: List of follower relationships in the format "user_1 user_2",
            indicating that user_1 follows user_2 (input type is string)
        Output: A list of user pairs <user_a, user_b> such that:
                - User_a follows user_b
                - User_b follows user_a
                - User_a has at least 10 followers
                - User_b has at least 10 followers
            Output key/value tuple format:
                Key: Mutual follower pair member with lesser id (int)
                Value: Mutual follower pair member with greater id (int)
    '''

    def mapper1(self, key, val):
        ojbk = REG.findall(val)
        for i,j in zip(ojbk[:1],ojbk[1:]):
            yield j,i

    def reducer1(self, key, vals):
        i=0
        emptylist = []
        for o in vals:
            i+=1
            emptylist.append((key,o))
        if i>=10:
            yield key,emptylist

    def mapper2(self,key,val):
        for j in val:
            for f in j:
                key = int(key)
                f = int (f)
                if key<f:
                    list = (key,f)
                    yield (list,1)
                elif key>f:
                    list = (f,key)
                    yield (list, 1)

    def reducer2(self,key,vals):
        i=0
        for _ in vals:
            i+=1
        if (i>=2):
            yield key[0],key[1]

    def steps(self):
        return [
            MRStep(mapper=self.mapper1, reducer=self.reducer1),
            MRStep(mapper=self.mapper2, reducer=self.reducer2)
        ]

if __name__ == '__main__':
    TwitterFollowers.SORT_VALUES = True
    TwitterFollowers.run()
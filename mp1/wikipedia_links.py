from mrjob.job import MRJob
import re
from mrjob.step import  MRStep
reg = r"[a-zA-Z_<1-9-]+"
REG = re.compile(reg)

class stru:
    def __init__(self,curr,prev):
        self.curr = curr
        self.prev = prev


class WikipediaLinks(MRJob):
    '''
        Input: List of lines, each containing a user session.
            - Articles are separated by ';' characters
            - Given a session 'page_a;page_b':
                this indicates there is a link from the article page_a to page_b
            - A '<' character indicates that a user has clicked 'back' on their
                browser and has returned to the previous page they were on
        Output: The number of unique inbound links to each article
            Key: Article name (str)
            Value: Number of unique inbound links (int)
    '''

    def mapper(self, key, val):
        ojbk = val.split(';')
        i = 0
        # prev1 record the last one, it's prev is updated as the last stru
        prev1 = stru(None,None)
        # prev record the last second one, not sure really need this or not
        for k in ojbk:
            if i==0:
                prev1.curr = k
                i+=1
                continue
            elif i==1:
                yield(k,prev1.curr)
                prev1 = stru(k,prev1)
                i+=1
                continue
            elif k=='<':
                prev1 = stru(prev1.prev.curr,prev1.prev.prev)
            else:
                yield(k,prev1.curr)
                prev1 = stru(k,prev1)




    # def reducer2(self, key, values):
    #     yield (key[0],1)

    def reducer(self,key,vals):
        i = 0
        tem = 0
        for k in vals:

            if k!= tem:
                i+=1
            tem = k
        yield (key,i)

    # def steps(self):
    #     return [
    #         MRStep(mapper=self.mapper,
    #                reducer=self.reducer2),
    #         MRStep(reducer=self.reducer)
    #     ]




if __name__ == '__main__':
    WikipediaLinks.SORT_VALUES = True
    WikipediaLinks.run()
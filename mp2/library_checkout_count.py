from mrjob.job import MRJob
import re
from mrjob.step import MRStep


ischeckout = re.compile(r"^[0-9]+,[0-9]+,")
n = r'^[0-9]+'
BIGNUM = re.compile(n)
YEAR = re.compile(r'[0-9]{4}')
TITLE = re.compile(r'.*?\.,')
class LibraryCheckoutCount(MRJob):
    '''
        Input: Records containing either checkout data or inventory data
        Output: A generated list of key/value tuples:
            Key: A book title and year, joined by a "|" character
            Value: The number of times that book was checked out in the given year
    '''

    def mapper(self, key, val):
        v = val.split(',')
        if len(v) == 6:
            bignum = v[0]
            year = YEAR.findall(v[5])
            if year:
                year = year[0]
                yield(bignum,(2,int(year)))
        else:
            title = v[1]
            bignum = v[0]
            # if it is a inventory entry, return (2,bignum) as key, (2, tiltle as value)
            yield (bignum,(1,title))


    def reducer(self, key, vals):
        # if it is a checkout entry , then return key: bignum, value:(2,year,totalcheckout)
        title = ""
        prev = 0
        i = 0
        for v in vals:
            # print (v)
            if v[0] == 1:
                title = v[1]
            if v[0] == 2:
                year = v[1]
                if prev ==0:
                    prev = year
                if prev == year:
                    i+=1
                else:
                    if title!="" and i!=0:
                        yield (title,(prev,i))
                        i = 1
                    prev = year
        if title!="" and i!=0:
            yield(title,(prev,i))

    def mapper2(self,key,value):
        yield key,value

    def reducer2(self,key,value):
        prevyear = 0
        i = 0
        for v in value:
            if prevyear==0:
                prevyear = v[0]
            if prevyear == v[0]:
                i+=v[1]
            else:
                yield key+"|"+str(prevyear),i
                prevyear = v[0]
                i = v[1]
        yield key+"|"+str(prevyear),i



    def steps(self):
        return [
            MRStep(mapper=self.mapper, reducer=self.reducer),
            MRStep(mapper =self.mapper2,reducer=self.reducer2)
        ]


if __name__ == '__main__':
    LibraryCheckoutCount.SORT_VALUES = True
    LibraryCheckoutCount.run()
from mrjob.job import MRJob
from mrjob.step import MRStep
import time
import re
class Part24(MRJob):

    def mapper(self, _,line):
        try:
            fields =line.split(",")
            value=float(fields[0])
            key= str(fields[1])
            yield(1,(value, key))
        except:
            pass

    def reducer(self,key,value):
        sorted_values =sorted(value, reverse=True, key= lambda x:x[0])[:10]
        count=0
        for i in sorted_values:
                count+=1
                yield(count,('{}-{}'.format(float(i[0]),str(i[1]).strip())))

if __name__ == '__main__':
    Part24.run()

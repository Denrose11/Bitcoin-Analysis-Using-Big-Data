
from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class part1(MRJob):
    def mapper(self, _, line):
        fields = line.split(",")
        try:
            if (len(fields)==5):
                time_epoch = int(fields[2])
                day = time.strftime("%Y,%m",time.gmtime(time_epoch))

                yield(day,1)
        except:
                pass

    def reducer(self, key,counts):
            y=sum(counts)
            yield(key,y)

if __name__ == '__main__':
    part1.run()

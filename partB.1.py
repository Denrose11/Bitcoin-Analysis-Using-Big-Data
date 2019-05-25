from mrjob.job import MRJob
import re
import time
WORD_REGEX = re.compile(r"\b\w+\b")
class part2(MRJob):

    def mapper(self, _, line):
        fields = line.split(",")
        try:
            hash = fields[0]
            value = fields[1]
            n = fields[2]
            publicKey = fields[3]
            a = fields[3]
            if (a == '{1HB5XMLmzFVj8ALj6mfBsbifRoD4miY36v}'):
                y=fields[0]+","+fields[1]+","+fields[2]+","+fields[3]
                print(y)
        except:
                pass
    #def reducer(self, key,counts):
    #        y=sum(counts)
    #        yield(key,y)

if __name__ == '__main__':
    part2.run()

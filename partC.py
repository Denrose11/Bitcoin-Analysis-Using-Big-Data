from mrjob.job import MRJob
from mrjob.step import MRStep
import datetime
import time
import re
class part21(MRJob):

    bit_coin = {}

    def mapper_join_init(self):
        with open("bitcoin_data.txt",) as f:
            for line in f:
                fields = line.split(",")
                key = fields [0]
                value = fields [1]
                self.bit_coin[key] = value

    def mapper_repl_join(self, _, line):
        try:
            fields = line.split(',')
            if(len(fields)==5):
                time_epoch = int(fields[2])
                day = time.strftime("%d-%m-%Y",time.gmtime(time_epoch))
                if day in self.bit_coin:
                    value= self.bit_coin[day]
                    print(fields[0]+','+fields[1]+','+day+','+fields[3]+','+fields[4]+','+value.strip())
        except:
            pass

    def steps(self):
        return [MRStep(mapper_init=self.mapper_join_init,
                          mapper=self.mapper_repl_join)]

if __name__ == '__main__':
    part21.run()

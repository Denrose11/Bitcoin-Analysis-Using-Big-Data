from mrjob.job import MRJob

from mrjob.step import MRStep



class part21(MRJob):



    bit_coin = {}



    def mapper_join_init(self):

        with open("ttttt.txt") as f:

            for line in f:

                fields = line.split(",")

                key = fields [0]

                self.bit_coin[key] = key



    def mapper_repl_join(self, _, line):

        try:

            fields = line.split(',')

            tx_id = fields[0]

            if (tx_id in self.bit_coin) :

                tx_hash = fields[1]

                vout = fields[2]

                combined = tx_id+","+tx_hash+","+vout

                print(combined)



        except:

            pass



    def steps(self):

        return [MRStep(mapper_init=self.mapper_join_init,

                          mapper=self.mapper_repl_join)]



if __name__ == '__main__':

    part21.run()

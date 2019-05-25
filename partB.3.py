from mrjob.job import MRJob

from mrjob.step import MRStep

class part2(MRJob):

    bit_coin = {}

    def mapper_join_init(self):

        with open("t1.txt") as f:

            for line in f:

                fields = line.split(",")

                key = fields [1]
                key1 = fields[2]

                self.bit_coin[key] = key1

    def mapper_repl_join(self, _, line):

        try:

            fields = line.split(",")

            hash = fields[0]
            value = fields[1]
            nval = fields[2]
            publickey = fields[3]


            if (hash in self.bit_coin) :

                if str(nval).strip() == str(self.bit_coin[hash]).strip():


                    combined = value+","+publickey

                    print(combined)

        except:
            pass

    def steps(self):

        return [MRStep(mapper_init=self.mapper_join_init,

                          mapper=self.mapper_repl_join)]

if __name__ == '__main__':
    part2.run()

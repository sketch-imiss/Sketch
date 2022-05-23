import mmh3
import random


class Partitioner:
    def __init__(self, data, partition, pnodes):
        self.data = data
        self.partition = partition
        self.pnodes = pnodes

    def cardinality_partition(self):
        partitioned_data = [[] for _ in range(self.pnodes)]

        # without partitioning
        if self.partition == 0:
            partitioned_data = self.data
        # random partitioning
        elif self.partition == 1:
            for pair in self.data:
                index = mmh3.hash(str(pair[0]) + '-' + str(pair[1]), signed=False) % self.pnodes
                partitioned_data[index].append(pair)
        # hash partitioning
        elif self.partition == 2:
            for pair in self.data:
                index = mmh3.hash(str(pair[0]), signed=False) % self.pnodes
                partitioned_data[index].append(pair)

        return partitioned_data

    def frequency_partition(self):
        partitioned_data = [[] for _ in range(self.pnodes)]

        # without partitioning
        if self.partition == 0:
            partitioned_data = self.data
        # random partitioning
        elif self.partition == 1:
            nuser = 0
            for pair in self.data:
                index = mmh3.hash(str(nuser), signed=False) % self.pnodes
                partitioned_data[index].append(pair)
                nuser += 1
        # hash partitioning
        elif self.partition == 2:
            for pair in self.data:
                index = mmh3.hash(str(pair[0]), signed=False) % self.pnodes
                partitioned_data[index].append(pair)

        return partitioned_data

    def persistency_partition(self):
        return 0
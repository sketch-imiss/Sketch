from sampler import Sampler
from partitioner import Partitioner


class DataLoader:
    def __init__(self, dataset, estimator, sample, sprobability, ssize, partition, pnodes):
        self.dataset = dataset
        self.estimator = estimator
        self.sample = sample
        self.sprobability = sprobability
        self.ssize = ssize
        self.partition = partition
        self.pnodes = pnodes

    def get_data(self):
        # sample a sub-dataset from the raw dataset

        sampler = Sampler(self.dataset, self.sample, self.sprobability, self.ssize)
        if self.estimator == 0:
            sampled_data, sprobability = sampler.cardinality_sample()
        elif self.estimator == 1:
            sampled_data, sprobability = sampler.frequency_sample()
        elif self.estimator == 2:
            sampled_data, sprobability = sampler.persistency_sample()

        partitioner = Partitioner(sampled_data, self.partition, self.pnodes)
        if self.estimator == 0:
            partitioned_data = partitioner.cardinality_partition()
        elif self.estimator == 1:
            partitioned_data = partitioner.frequency_partition()
        elif self.estimator == 2:
            partitioned_data = partitioner.persistency_partition()

        return partitioned_data, sprobability
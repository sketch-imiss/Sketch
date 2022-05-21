from sampler import Sampler


class DataLoader:
    def __init__(self, dataset, estimator, sample, sprobability, ssize, partition, pnodes):
        self.dataset = dataset
        self.estimator = estimator
        self.sample = sample
        self.sprobability = sprobability
        self.ssize = ssize
        self.partition = partition
        self.pnodes = pnodes

    def get_dataset(self):
        # sample a sub-dataset from the raw dataset

        sampler = Sampler(self.dataset, self.sample, self.sprobability, self.ssize)
        if self.estimator == 0:
            sampler.cardinality_sample()
        elif self.estimator == 1:
            sampler.frequency_sample()
        elif self.estimator == 2:
            sampler.persistency_sample()
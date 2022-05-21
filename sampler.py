import logging


class Sampler:
    def __init__(self, dataset, sample, sprobability, ssize):
        self.dataset = dataset
        self.sample = sample
        self.sprobability = sprobability
        self.ssize = ssize

    def cardinality_sample(self):
        sampled_dataset = []

        # without sampling
        if self.sample == 0:
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
                    sampled_dataset.append((user, item))
        # fixed probability sampling
        elif self.sample == 1:
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
        # reservoir sampling
        elif self.sample == 2:
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
        # sample and hold
        elif self.sample == 3:
            logging.info('==> TO DO')

        return sampled_dataset

    def frequency_sample(self):
        return 0

    def persistency_sample(self):
        return 0
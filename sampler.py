import logging
import mmh3
import random


denominator = 2 ** 32 - 1


class Sampler:
    def __init__(self, dataset, sample, sprobability, ssize):
        self.dataset = dataset
        self.sample = sample
        self.sprobability = sprobability
        self.ssize = ssize

    def cardinality_sample(self):
        sampled_data = []

        # without sampling
        if self.sample == 0:
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
                    sampled_data.append((user, item))
        # fixed probability sampling
        elif self.sample == 1:
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
                    random_num = mmh3.hash(str(user) + '-' + str(item), signed=False) / denominator
                    if random_num < self.sprobability:
                        sampled_data.append((user, item))
        # reservoir sampling
        elif self.sample == 2:
            npair = 0
            with open(self.dataset, 'r') as f:
                for line in f:
                    [user, item] = list(map(int, line.strip().split()))
                    npair += 1
                    if len(sampled_data) < self.ssize:
                        sampled_data.append((user, item))
                    else:
                        random_num = mmh3.hash(str(user) + '-' + str(item), signed=False) / denominator
                        if random_num < self.ssize / npair:
                            random_index = random.randint(0, self.ssize+1)
                            del sampled_data[random_index]
                            sampled_data.append((user, item))
            self.sprobability = self.ssize / npair
        # sample and hold
        elif self.sample == 3:
            logging.info('==> TO DO')

        return sampled_data

    def frequency_sample(self):
        return 0

    def persistency_sample(self):
        return 0
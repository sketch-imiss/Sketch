import logging
import pickle
import mmh3
import random
import math
from scipy.stats import beta

phi = 0.77351


class Frequency:
    def __init__(self, dataset, filter_size, threshold, fhash, row, column, output, fastupdate):
        self.dataset = dataset
        self.filter_size = filter_size
        self.threshold = threshold
        self.fhash = fhash
        self.row = row
        self.column = column
        self.chash = row
        self.output = output
        self.fastupdate = fastupdate

    def initialize(self):
        self.filter = [0] * self.filter_size
        self.cm_cu = [0] * (self.row * self.column)
        self.cm_cu_size = self.row * self.column
        self.f = 0
        self.set_user = set()

    def basic_update(self):
        with open(self.dataset) as freader:
            for line in freader:
                [user, weight] = list(map(int, line.strip().split()))
                self.set_user.add(user)
                lst_findex = [mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter_size
                             for i in range(self.fhash)]
                for _ in range(weight):
                    gamma = min([self.filter[index] for index in lst_findex])
                    if gamma < self.threshold:
                        self.f += 1
                        for i in range(self.fhash):
                            random_num = math.floor(-math.log2(random.random()))
                            self.filter[lst_findex[i]] = max(min(random_num, self.threshold), self.filter[lst_findex[i]])
                    else:
                        self.cm_cu_update(user, 1)

    def cm_cu_update(self, user, increment):
        lst_counter = list()
        lst_cindex = list()
        for j in range(self.chash):
            index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
            lst_counter.append(self.cm_cu[index])
            lst_cindex.append(index)
        min_counter = min(lst_counter)
        for index in lst_cindex:
            if self.cm_cu[index] == min_counter:
                self.cm_cu[index] += increment

    def fast_update(self):
        with open(self.dataset) as freader:
            for line in freader:
                [user, weight] = list(map(int, line.strip().split()))
                self.set_user.add(user)
                lst_findex = list()
                gamma = 2 ** 32 - 1
                for i in range(self.fhash):
                    findex = mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter
                    lst_findex.append(findex)
                    gamma = min(gamma, self.filter[findex])
                if gamma < self.threshold:
                    random_num = math.floor(-math.log2(beta.rvs(1, weight)))
                    if random_num <= self.threshold:
                        self.f += weight
                        for findex in lst_findex:
                            self.filter[findex] = max(self.filter[findex], random_num)
                    else:
                        for findex in lst_findex:
                            self.filter[findex] = self.threshold
                        mu = random.random()
                        flag = math.ceil(math.log(1 - mu * (1 - (1 - 2 ** (-self.threshold)) ** weight))
                                         / math.log(1 - 2 ** (-self.threshold)))
                        self.f += flag
                        self.cm_cu_update(user, weight-flag)
                else:
                    self.cm_cu_update(user, weight)

    def estimate(self):
        dict_frequency = dict()
        for user in self.set_user:
            lst_findex = [mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter_size
                          for i in range(self.fhash)]
            gamma = min([self.filter[index] for index in lst_findex])
            if gamma < self.threshold:
                filter_ave = sum([self.filter[findex] for findex in lst_findex])
                frequency = self.filter_size * self.fhash * \
                            ((2 ** filter_ave) / (phi * self.fhash) - self.f / self.filter_size)\
                            / (self.filter - self.fhash)
                dict_frequency[user] = frequency
            else:
                cfrequency = 2 ** 32 - 1
                for j in self.chash:
                    index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
                    cfrequency = min(cfrequency, self.cm_cu[index])
                ffrequency = self.filter_size * self.fhash * \
                             ((2 ** self.threshold / (phi * self.fhash)) - (self.f / self.filter_size)) \
                             / (self.filter_size - self.fhash)
                frequency = cfrequency + ffrequency
                dict_frequency[user] = frequency

        writer = open(self.output, 'wb')
        pickle.dump(dict_frequency, writer)
        writer.close()

    def run(self):
        self.initialize()
        if self.fastupdate == 0:
            self.basic_update()
            self.estimate()
        elif self.fastupdate == 1:
            self.fast_update()
            self.estimate()
        else:
            logging.error('Please select the correct update category: 0 for basic update / 1 for fast update')
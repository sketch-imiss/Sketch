import pickle
import mmh3
import random
import math
from scipy.stats import beta
import ray

from aggregator import *


# start ray
ray.init()

phi = 0.77351


class Frequency:
    def __init__(self, data, sample, sprobability, partition, filter_size, threshold, fhash, row, column, output,
                 fastupdate):
        self.data = data
        self.sample = sample
        self.sprobability = sprobability
        self.partition = partition
        self.filter_size = filter_size
        self.threshold = threshold
        self.fhash = fhash
        self.row = row
        self.column = column
        self.cm_cu_size = self.row * self.column
        self.chash = row
        self.output = output
        self.fastupdate = fastupdate

    @ray.remote
    def basic_update(self, data):
        filter = [0] * self.filter_size
        cm_cu = [0] * self.cm_cu_size
        f = 0
        set_user = set()

        for pair in data:
            user, weight = pair[0], pair[1]
            set_user.add(user)
            lst_findex = [mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter_size
                          for i in range(self.fhash)]
            for _ in range(weight):
                gamma = min([filter[index]] for index in lst_findex)
                # update the filter
                if gamma < self.threshold:
                    f += 1
                    for i in range(self.fhash):
                        random_num = math.floor(-math.log2(random.random()))
                        filter[lst_findex[i]] = max(min(random_num, self.threshold), filter[lst_findex[i]])
                # update the cm_cu sketch
                else:
                    lst_counter = list()
                    lst_cindex = list()
                    for j in range(self.chash):
                        index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
                        lst_counter.append(cm_cu[index])
                        lst_cindex.append(index)
                    min_counter = min(lst_counter)
                    for index in lst_cindex:
                        if cm_cu[index] == min_counter:
                            cm_cu[index] += 1

        return filter, cm_cu, f, set_user

    @ray.remote
    def fast_update(self, data):
        filter = [0] * self.filter_size
        cm_cu = [0] * self.cm_cu_size
        f = 0
        set_user = set()

        for pair in data:
            user, weight = pair[0], pair[1]
            set_user.add(user)
            lst_findex = list()
            gamma = 2 ** 32 - 1
            for i in range(self.fhash):
                findex = mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter_size
                lst_findex.append(findex)
                gamma = min(gamma, filter[findex])
            if gamma < self.threshold:
                random_num = math.floor(-math.log2(beta.rvs(1, weight)))
                if random_num <= self.threshold:
                    f += weight
                    for findex in lst_findex:
                        filter[findex] = max(filter[findex], random_num)
                else:
                    for findex in lst_findex:
                        filter[findex] = self.threshold
                    mu = random.random()
                    flag = math.ceil(math.log(1 - mu * (1 - (1 - 2 ** (-self.threshold)) ** weight))
                                     / math.log(1 - 2 ** (-self.threshold)))
                    f += flag
                    lst_counter = list()
                    lst_cindex = list()
                    for j in range(self.chash):
                        index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
                        lst_counter.append(cm_cu[index])
                        lst_cindex.append(index)
                    min_counter = min(lst_counter)
                    for index in lst_cindex:
                        if cm_cu[index] == min_counter:
                            cm_cu[index] += (weight - flag)
            else:
                lst_counter = list()
                lst_cindex = list()
                for j in range(self.chash):
                    index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
                    lst_counter.append(cm_cu[index])
                    lst_cindex.append(index)
                min_counter = min(lst_counter)
                for index in lst_cindex:
                    if cm_cu[index] == min_counter:
                        cm_cu[index] += weight

    @ray.remote
    def estimate(self, filter, cm_cu, f, set_user):
        dict_frequency = dict()
        for user in set_user:
            lst_findex = [mmh3.hash(str(user) + '-' + str(i), signed=False) % self.filter_size
                          for i in range(self.fhash)]
            gamma = min([filter[index] for index in lst_findex])
            if gamma < self.threshold:
                filter_ave = sum([filter[findex] for findex in lst_findex])
                frequency = self.filter_size * self.fhash * \
                            ((2 ** filter_ave) / (phi * self.fhash) - f / self.filter_size)\
                            / (self.filter_size - self.fhash)
                dict_frequency[user] = frequency
            else:
                cfrequency = 2 ** 32 - 1
                for j in self.chash:
                    index = mmh3.hash(str(user) + '-' + str(j), signed=False) % self.cm_cu_size
                    cfrequency = min(cfrequency, cm_cu[index])
                ffrequency = self.filter_size * self.fhash * \
                             ((2 ** self.threshold / (phi * self.fhash)) - (f / self.filter_size)) \
                             / (self.filter_size - self.fhash)
                frequency = cfrequency + ffrequency
                dict_frequency[user] = frequency

        return dict_frequency

    def run(self):
        # basic update version
        if self.fastupdate == 0:
            if self.partition == 0:
                filter, cm_cu, f, set_user = self.basic_update(self.data)
                result = self.estimate(filter, cm_cu, f, set_user)
            elif self.partition == 1:
                tmp = list()
                for i in range(len(self.data)):
                    filter, cm_cu, f, set_user = self.basic_update.remote(self.data[i])
                    dict_frequency = self.estimate.remote(filter, cm_cu, f, set_user)
                    tmp.append(dict_frequency)
                result = ray.get(tmp)
        # fast update version
        elif self.fastupdate == 1:
            if self.partition == 0:
                filter, cm_cu, f, set_user = self.fast_update(self.data)
                result = self.estimate(filter, cm_cu, f, set_user)
            elif self.partition == 1:
                tmp = list()
                for i in range(len(self.data)):
                    filter, cm_cu, f, set_user = self.fast_update(self.data[i])
                    dict_frequency = self.estimate.remote(filter, cm_cu, f, set_user)
                    tmp.append(dict_frequency)
                result = ray.get(tmp)
        else:
            print('Please select the correct update category: 0-basic update, 1-fast update')

        dict_result = aggregator(result, self.partition, self.sample, self.sprobability)

        writer = open(self.output, 'wb')
        pickle.dump(dict_result, writer)
        writer.close()
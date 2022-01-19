import logging
import pickle
import mmh3
import random
import math


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
                        min_value = 2 ** 32 - 1
                        min_index = 0


    def fast_update(self):
        print('fast_update')

    def run(self):
        self.initialize()
        if self.fastupdate == 0:
            self.basic_update()
        elif self.fastupdate == 1:
            self.fast_update()
        else:
            logging.error('Please select the correct update category: 0 for basic update / 1 for fast update')
import logging
import pickle


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

    def basic_update(self):
        print('basic_update')

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
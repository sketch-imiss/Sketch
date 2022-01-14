import pickle


class Frequency:
    def __init__(self, dataset, filter_size, threshold, fhash, row, column, output):
        self.dataset = dataset
        self.filter_size = filter_size
        self.threshold = threshold
        self.fhash = fhash
        self.row = row
        self.column = column
        self.chash = row
        self.output = output

    def initialize(self):
        self.filter = [0] * self.filter_size
        self.cm_cu = [0] * (self.row * self.column)

    def update(self):
        print(123)

    def run(self):
        self.initialize()
        self.update()
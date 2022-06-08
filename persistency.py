class Persistency:
    def __init__(self, dataset, sk_size, timeslot, reversible, output):
        self.dataset = dataset
        self.sk_size = sk_size
        self.timeslot = timeslot
        self.reversible = reversible
        self.output = output

    def bf_update(self):
        print('==> TO DO')

    def reversible_bf_update(self):
        print('==> TO DO')

    def run(self):
        if self.reversible == 0:
            self.bf_update()
        elif self.reversible == 1:
            self.reversible_bf_update()
        else:
            print('Please select the correct reversibility: 0-bloom filter, 1-reversible bloom filter')
class Persistency:
    def __init__(self, dataset, sk_size, timeslot, reversible):
        self.dataset = dataset
        self.sk_size = sk_size
        self.timeslot = timeslot
        self.reversible = reversible

    def run(self):
        print(123)
import mmh3


class Cardinality:
    def __init__(self, dataset, sk_size, bucket_type):
        self.dataset = dataset
        self.sk_size = sk_size
        self.bucket_type = bucket_type

    def run(self):
        self.sketch = [0] * self.sk_size
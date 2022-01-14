import mmh3
import pickle
import time
import math


class Cardinality:
    def __init__(self, dataset, sk_size, bucket_type, output):
        self.dataset = dataset
        self.sk_size = sk_size
        self.bucket_type = bucket_type
        self.output = output

    def geometric_hash(self, input):
        bin_str = '0' * (32 - len(bin(input)[2:])) + bin(input)[2:]
        b = math.ceil(math.log(self.sk_size) / math.log(2))
        index = int(bin_str[0:b], 2) % self.sk_size
        print(bin_str[b:])
        value = 0
        for bit in bin_str[b:]:
            value += 1
            if bit == '1':
                break
        return index, value

    def bit_estimator(self):
        sketch = [0] * self.sk_size
        num_zerobit = self.sk_size
        dict_cardinality = dict()

        with open(self.dataset) as reader:
            for line in reader:
                [user, item] = list(map(int, line.strip().split()))
                index = mmh3.hash(str(user) + '-' + str(item), signed=False) % self.sk_size
                if user not in dict_cardinality:
                    dict_cardinality[user] = 0
                if sketch[index] == 0:
                    sketch[index] = 1
                    dict_cardinality[user] += (self.sk_size / num_zerobit)
                    num_zerobit -= 1
        reader.close()

        writer = open(self.output, 'wb')
        pickle.dump(dict_cardinality, writer)
        writer.close()

    def register_estimator(self):
        sketch = [0] * self.sk_size
        dict_cardinality = dict()
        update_prob = 1

        with open(self.dataset) as reader:
            for line in reader:
                [user, item] = list(map(int, line.strip().split()))
                if user not in dict_cardinality:
                    dict_cardinality[user] = 0
                input = mmh3.hash(str(user) + '-' + str(item), signed=False)
                index, value = self.geometric_hash(input)
                if value > sketch[index]:
                    dict_cardinality[user] += (1 / update_prob)
                    update_prob += ((2 ** (-value) - 2 ** (-sketch[index])) / self.sk_size)
                    sketch[index] = value
        reader.close()

        writer = open(self.output, 'wb')
        pickle.dump(dict_cardinality, writer)
        writer.close()

    def run(self):
        if self.bucket_type == 'bit':
            self.bit_estimator()
        elif self.bucket_type == 'register':
            self.register_estimator()
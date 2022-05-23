import mmh3
import ray
import pickle
import math

from utils import *

# start ray
ray.init()


class Cardinality:
    def __init__(self, data, sample, sprobability, partition, sk_size, sk_type, output):
        self.data = data
        self.sample = sample
        self.sprobability = sprobability
        self.partition = partition
        self.sk_size = sk_size
        self.sk_type = sk_type
        self.output = output

    # compute the index and hash value for the register-based estimator
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

    @ray.remote
    def bit_estimator(self, data):
        sketch = [0] * self.sk_size
        num_zerobit = self.sk_size
        dict_cardinality = dict()

        for pair in data:
            user, item = pair[0], pair[1]
            index = mmh3.hash(str(user) + '-' + str(item), signed=False) % self.sk_size
            if user not in dict_cardinality:
                dict_cardinality[user] = 0
            if sketch[index] == 0:
                sketch[index] = 1
                dict_cardinality[user] += (self.sk_size / num_zerobit)
                num_zerobit -= 1

        return dict_cardinality

    @ray.remote
    def register_estimator(self, data):
        sketch = [0] * self.sk_size
        dict_cardinality = dict()
        update_prob = 1

        for pair in data:
            user, item = pair[0], pair[1]
            if user not in dict_cardinality:
                dict_cardinality[user] = 0
            input = mmh3.hash(str(user) + '-' + str(item), signed=False)
            index, value = self.geometric_hash(input)
            if value > sketch[index]:
                dict_cardinality[user] += (1 / update_prob)
                update_prob += ((2 ** (-value) - 2 ** (-sketch[index])) / self.sk_size)
                sketch[index] = value

        return dict_cardinality

    def run(self):
        # bit-based estimator
        if self.sk_type == 0:
            if self.partition == 0:
                result = self.bit_estimator(self.data)
            elif self.partition == 1:
                tmp = list()
                for i in range(len(self.data)):
                    dict_cardinality = self.bit_estimator.remote(self.data[i])
                    tmp.append(dict_cardinality)
                result = ray.get(tmp)
        # register-based estimator
        elif self.sk_type == 1:
            if self.partition == 0:
                result = self.register_estimator(self.data)
            elif self.partition == 1:
                tmp = list()
                for i in range(len(self.data)):
                    dict_cardinality = self.register_estimator.remote(self.data[i])
                    tmp.append(dict_cardinality)
                result = ray.get(tmp)
        else:
            print('Please select the correct bucket type: 0-bit, 1-register')

        dict_result = aggregator(result, self.partition, self.sample, self.sprobability)

        writer = open(self.output, 'wb')
        pickle.dump(dict_result, writer)
        writer.close()
import argparse
import logging

from dataloader import DataLoader

from cardinality import Cardinality
from frequency import Frequency
from persistency import Persistency


def get_args():
    parser = argparse.ArgumentParser(description='a Python library for monitoring large-scale data streams')

    # input & output, estimator
    parser.add_argument('--dataset', default='dataset/cardinality.dat', type=str, help='dataset path')
    parser.add_argument('--estimator', default=0, type=int, help='estimation task: 0-cardinality, 1-frequency, 2-persistency')
    parser.add_argument('--output', default='results/result.out', type=str, help='output path')

    # sampling setting
    parser.add_argument('--sample', default=0, type=int, help='sampling strategy: 0-without sampling, 1-fixed probability, 2-reservoir, 3-sample and hold')
    parser.add_argument('--sprobability', default=1, type=float, help='sampling probability')
    parser.add_argument('--ssize', default=1000, type=int, help='sampling size')

    # partitioning setting
    parser.add_argument('--partition', default=0, type=int, help='partitioning strategy: 0-without partitioning, 1-random partition, 2-hash partition')
    parser.add_argument('--pnodes', default=1, type=int, help='the number of partitions')

    # cardinality estimation parameters
    parser.add_argument('--csize', default=100000, type=int, help='sketch size for cardinality estimation')
    parser.add_argument('--ctype', default=0, type=int, help='type of each bucket: 0-bit, 1-register')

    # frequency estimation parameters
    parser.add_argument('--fsize', default=10000, type=int, help='filter size for frequency estimation')
    parser.add_argument('--fthreshold', default=10, type=int, help='filter threshold for frequency estimation')
    parser.add_argument('--fhash', default=3, type=int, help='the number of hash functions in the filter')
    parser.add_argument('--frow', default=3, type=int, help='the number of rows in cm-cu sketch')
    parser.add_argument('--fcolumn', default=10000, type=int, help='the number of columns in cm-cu sketch')
    parser.add_argument('--fastupdate', default=0, type=int, help='whether fast update the sketch or not')

    # persistency estimation parameters
    parser.add_argument('--psize', default=100000, type=int, help='sketch size for persistency estimation')
    parser.add_argument('--ptime', default=30, type=int, help='the size of each timeslot')
    parser.add_argument('--preversible', default=0, type=int, help='whether the sketch is reversible or not')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()

    dataloader = DataLoader(args.dataset, args.estimator, args.sample, args.sprobability, args.ssize, args.partition, args.pnodes)

    print(args.estimator)
    if args.estimator == 'cardinality':
        estimator = Cardinality(args.dataset, args.csize, args.ctype, args.output)
        estimator.run()
    elif args.estimator == 'frequency':
        estimator = Frequency(args.dataset, args.fsize, args.fthreshold, args.fhash, args.frow, args.fcolumn,
                              args.output, args.fastupdate)
        estimator.run()
    elif args.estimator == 'persistency':
        estimator = Persistency(args.dataset, args.psize, args.ptime, args.preversible, args.output)
        estimator.run()
    else:
        logging.error('Please select the correct estimation task: cardinality/frequency/persistency')
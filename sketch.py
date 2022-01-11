import argparse
import logging

from cardinality import Cardinality
from frequency import Frequency
from persistency import Persistency


def get_args():
    parser = argparse.ArgumentParser(description='a sketch library to fast estimating metrics over data streams')
    parser.add_argument('--dataset', default='dataset/demo.dat', type=str, help='dataset path')
    parser.add_argument('--estimator', default='cardinality', type=str, help='estimation task')
    parser.add_argument('--output', default='results/result.out', type=str, help='output path')

    # cardinality estimation parameters
    parser.add_argument('--csize', default=100000, type=int, help='sketch size for cardinality estimation')
    parser.add_argument('--ctype', default='bit', type=str, help='type of each bucket in the sketch')

    # frequency estimation parameters
    parser.add_argument('--fsize', default=1000, type=int, help='filter size for frequency estimation')
    parser.add_argument('--fthreshold', default=10, type=int, help='filter threshold for frequency estimation')
    parser.add_argument('--frow', default=100, type=int, help='the number of rows in cm-cu sketch')
    parser.add_argument('--fcolumn', default=100, type=int, help='the number of columns in cm-cu sketch')

    # persistency estimation parameters
    parser.add_argument('--psize', default=100000, type=int, help='sketch size for persistency estimation')
    parser.add_argument('--ptime', default=30, type=int, help='the size of each timeslot')
    parser.add_argument('--preversible', default=0, type=int, help='whether the sketch is reversible or not')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    args = get_args()
    if args.estimator == 'cardinaltiy':
        estimator = Cardinality(args.dataset, args.card_size, args.bucket_type)
        estimator.run()
    elif args.estimator == 'frequency':
        estimator = Frequency(args.dataset, args.filter_size, args.filter_threshold, args.cm_row, args.cm_column)
        estimator.run()
    elif args.estimator == 'persistency':
        estimator = Persistency(args.dataset, args.per_size, args.timeslot, args.reversible)
        estimator.run()
    else:
        logging.error('Please select the correct estimation task: cardinality/frequency/persistency')
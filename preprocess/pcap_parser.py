import argparse
import struct


def get_args():
    parser = argparse.ArgumentParser(description='parser for parsing pcap files')
    parser.add_argument('--dataset', type=str, help='pcap file path')
    parser.add_argument('--output', type=str, help='output path')

    args = parser.parse_args()

    return args


if __name__ == '__main__':
    args = get_args()

    file = open(args.dataset, 'rb')
    data = file.read()

    i = 24
    j = 40
    k = 54
    l = 74

    while i < len(data):
        # timestamp
        seconds = struct.unpack('I', data[i:i + 4])[0]
        microseconds = float(struct.unpack('I', data[i + 4:i + 8])[0]) / 1000000
        timestamp = seconds + microseconds

        length = struct.unpack('I', data[i + 8:i + 12])[0]
        type = data[j + 12:j + 14]

        # protocol, ip, and port
        if type == '\x08\x00':
            protocol = struct.unpack('B', data[k + 9:k + 10])[0]
            sip = str(struct.unpack('B', data[k + 12:k + 13])[0]) + '.' + str(
                struct.unpack('B', data[k + 13:k + 14])[0]) + \
                  '.' + str(struct.unpack('B', data[k + 14:k + 15])[0]) + '.' + str(
                struct.unpack('B', data[k + 15:k + 16])[0])
            dip = str(struct.unpack('B', data[k + 16:k + 17])[0]) + '.' + str(
                struct.unpack('B', data[k + 17:k + 18])[0]) + \
                  '.' + str(struct.unpack('B', data[k + 18:k + 19])[0]) + '.' + str(
                struct.unpack('B', data[k + 19:k + 20])[0])
            sport = struct.unpack('H', data[l + 1:l + 2] + data[l:l + 1])[0]
            dport = struct.unpack('H', data[l + 3:l + 4] + data[l + 2:l + 3])[0]

        i = i + length + 16
        j = j + length + 16
        k = k + length + 16
        l = l + length + 16

    file.close()
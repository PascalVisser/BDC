#! usr/bin/env python3

"""
Script that checks if a given fastq RNAseq file is 'valid'
"""


__author__ = "Pascal Visser"
__version__ = 2.2


import sys
import csv


def read_fastq(fastqfile):
    """Reads the fastq file and calcualates the phredscores"""

    # open file
    with open(fastqfile, 'r') as fastq:
        valid = True
        counter = 0
        min_length = 1e99
        max_length = 0
        avg_length = [0, 0]
        while True:
            header = fastq.readline().rstrip()
            nucleotides = fastq.readline().rstrip()
            seperator = fastq.readline().rstrip()
            qualilty = fastq.readline().rstrip()

            if len(header) == 0:
                break
            if len(nucleotides) == 0:
                counter += 1
                break
            if len(seperator) == 0:
                counter += 2
                break
            if len(qualilty) == 0:
                counter += 3
                break
            else:
                counter += 4

            # validity checks only if it is still valid
            if valid:
                if len(qualilty) != len(nucleotides):
                    valid = False
                if not header.startswith('@'):
                    valid = False

            # keep track of lengths:
            if len(nucleotides) < min_length:
                min_length = len(nucleotides)
            if len(nucleotides) > max_length:
                max_length = len(nucleotides)
            avg_length[0] += len(nucleotides)
            avg_length[1] += 1

        # return values
        file = fastqfile.split('/')[-1]
        return {'file': file, 'valid': valid, 'min_length': min_length, 'max_length': max_length,
                'avg_length': avg_length[0] / avg_length[1]}


if __name__ == '__main__':
    result = read_fastq(sys.argv[1])
    # write rows
    csv.writer(sys.stdout, delimiter=',').writerow(
        [result['file'], result['valid'], result['min_length'], result['max_length'], result['avg_length']])

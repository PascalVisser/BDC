#! usr/bin/env python3


"""
Calculates the average phredscore within a fastq file
"""

__author__ = "Pascal Visser"
__version__ = 2.0

import sys
import time
import csv
import argparse as ap
import multiprocessing as mp


def read_fastq_file(fastq_file):
    """Reads the files and pick the quality line"""
    quality_line = []
    quality = True

    with open(fastq_file, encoding='UTF-8') as fastq:
        while quality:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            quality = fastq.readline().rstrip()

            if quality:
                quality_line.append(quality)
            elif header:
                pass
            elif nucleotides:
                pass
            elif strand:
                pass

    return quality_line


def make_chunks(number, size):
    """Make processable chunks of the data"""
    chunks = []

    for i in range(size):
        start = int(i * len(number) / size)
        end = int((i + 1) * len(number) / size)
        chunks.append(number[start:end])
    return chunks


def calc_quality_score(quality):
    """Calculates quality scores of the quality line"""
    result = []
    for quality_line in quality:
        for item, checker in enumerate(quality_line):
            try:
                result[item] += ord(checker) - 33
            except IndexError:
                result.append(ord(checker) - 33)
    return result


def get_output(average_phredscores, csvfile):
    """ processes the output for the file(s) """
    if csvfile is None:
        csv_writer = csv.writer(sys.stdout, delimiter=',')
        for i, score in enumerate(average_phredscores):
            csv_writer.writerow([i, score])

    else:
        with open(csvfile, 'a', encoding='UTF-8', newline='') as myfastq:
            csv_writer = csv.writer(myfastq, delimiter=',')
            for i, score in enumerate(average_phredscores):
                csv_writer.writerows(zip([i], [score]))


def main(argv=None):
    """Main function"""
    pass


    
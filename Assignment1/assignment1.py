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
        with open(csvfile, 'w', encoding='UTF-8', newline='') as myfastq:
            csv_writer = csv.writer(myfastq, delimiter=',')
            for i, score in enumerate(average_phredscores):
                csv_writer.writerow([i, score])


def main(argv=None):
    """Executing function"""

    # Collect input with argparse
    argparser = ap.ArgumentParser(description="Script voor Opdracht 1 van Big Data Computing")

    argparser.add_argument("-n", action="store",
                           dest="n", required=True, type=int,
                           help="Aantal cores om te gebruiken.")

    argparser.add_argument("--output", "-o",
                           required=False,
                           help="CSV file om de output in op te slaan. "
                                "Default is output naar terminal STDOUT")

    argparser.add_argument("fastq_files", action="store", nargs='+',
                           help="Minstens 1 Illumina Fastq Format file om te verwerken")
    args = argparser.parse_args()

    # start timing
    start = time.time()

    # loop through the input file(s) to make chunks
    for fastq in args.fastq_files:
        qualities = read_fastq_file(fastq)
        qual_chunked = make_chunks(qualities, 100)

        # use multiple processes to calculate the phredscores
        with mp.Pool(args.n) as pool:
            phredscores = pool.map(calc_quality_score, qual_chunked)

        phredscores_avg = [sum(i) / len(qualities) for i in zip(*phredscores)]

        # Determine what to do with the (csv)output
        if len(args.fastq_files) > 1:
            if args.CSVfile is None:
                sys.stdout.write(fastq + "\n")
                csv_file = None
            else:
                csv_file = f'{fastq}.{args.CSVfile}'
        else:
            csv_file = args.CSVfile

        get_output(phredscores_avg, csv_file)

        # End timing
        end = time.time()

        # print total proces time
        print(f"\nFinished in {round(end - start, 2)} seconds\n")


if __name__ == '__main__':
    sys.exit(main(sys.argv))

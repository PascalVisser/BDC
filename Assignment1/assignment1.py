#!/usr/bin/env python3


"""
Script to transform .fastq files to an output with phredscores only
"""


__author__ = "Pascal Visser"
__version__ = 1.2


import sys
import argparse as ap


def process_fasta(fastqfile, start=0, chunk=0):
    """open file(s) and calculate the phredscores"""
    with open(fastqfile, 'r', encoding='utf8') as fastq:
        if chunk == 0:
            chunk = len(fastq.readlines()) - 1
        fastq.seek(0)
        # fastforward
        # start = 0
        i = 0
        while i < start:
            fastq.readline()
            i += 1

        results = []
        counter = 0
        while counter < chunk:
            header = fastq.readline()
            nucleotides = fastq.readline()
            strand = fastq.readline()
            qual = fastq.readline()
            counter += 4

            if not qual:
                # we reached the end of the file
                break
            for j, c in enumerate(qual):

                try:
                    results[j] += ord(c) - 33
                except IndexError:
                    results.append(ord(c) - 33)

    score = [(phredscore / (counter / 4)) for phredscore in results]
    return score


def write_csvfile(phredscores, csvfile="phredscores.csv"):
    """Write results to .csv file"""
    with open(csvfile, 'x', encoding='utf8') as csv:
        for number in range(len(phredscores) - 1):
            csv.write(str(number) + ',' + str(phredscores[number]) + "\n")


def main(args=None):
    """Main function"""

    # argument parser
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

    # Assign arguments to variables
    fastqfiles = args.fastq_files
    csvname = str(args.output)

    # Execute functions on the arguments
    for readfile in fastqfiles:
        scores = process_fasta(readfile)

    if csvname == 'None':
        for number in range(len(scores) - 1):
            sys.stdout.write(str(number) + ',' + str(scores[number]) + "\n")
    else:
        write_csvfile(scores, csvname)


if __name__ == '__main__':
    sys.exit(main(sys.argv))

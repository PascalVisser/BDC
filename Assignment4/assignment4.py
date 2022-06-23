#! usr/bin/env python3

"""
Script that checks if a given fastq RNAseq file is 'valid'
"""


__author__ = "Pascal Visser"
__version__ = 1.2


import argparse
import sys
from statistics import mean


def fastq_control_centre(fastq):
    """ Controls the Fastq files. """

    complete = True
    quals = ""
    start = ""
    count_l = 0
    len_line = []

    with open(fastq[0], "r", encoding="utf-8") as myfile:
        for item, line in enumerate(myfile):
            count_l += 1
            count = item - 1
            if not count % 4:
                start = len(line.strip())
                len_line.append(start)

            count = item + 1
            if not count % 4:
                quals = len(line.strip())
                len_line.append(quals)

            if quals and start:
                if start != quals:
                    complete = False
                break

            if count_l % 4:
                complete = False
            else:
                pass

    maxed_lines = str(max(len_line))
    min_lines = str(min(len_line))

    line_length_mean = str(round(mean(len_line), 2))

    files = sys.stdout.write(str(fastq[0]).split("/", maxsplit=1)[-1])
    sys.stdout.write(str(files) + "," + str(complete) + "," + min_lines +
                     "," + maxed_lines + "," + line_length_mean + "\n")


def argument_parser():
    """ Arg-parser for commandline. """

    myargs = argparse.ArgumentParser(description="Script voor Opdracht 4 van Big Data Computing.")
    myargs.add_argument("fastq", action="store", type=str, nargs='*',
                        help="Minstens 1 Illumina Fastq Format file om te verwerken")

    args = myargs.parse_args()
    main(args)


def main(args):
    """ Main function, runs entire script. """
    fastq_control_centre(args.fastq)


if __name__ == '__main__':
    argument_parser()

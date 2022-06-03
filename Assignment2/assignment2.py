#! usr/bin/env python3


"""
main script
"""

from server_manager import runserver
from client_manager import runclient
from phredscore_calc import read_fastq_file, make_chunks, calc_quality_score, get_output

import argparse as ap
import multiprocessing as mp
import sys
import time


def main(args=None):

    argparser = ap.ArgumentParser(
        description="Script voor Opdracht 2 van Big Data Computing;  Calculate PHRED scores over the network.")
    mode = argparser.add_mutually_exclusive_group(required=True)

    mode.add_argument("-s", action="store_true", help="Run the program in Server mode; see extra options needed below")
    mode.add_argument("-c", action="store_true", help="Run the program in Client mode; see extra options needed below")

    server_args = argparser.add_argument_group(title="Arguments when run in server mode")
    server_args.add_argument("-o", action="store", dest="csvfile", type=str,
                             required=False,
                             help="CSV file om de output in op te slaan. Default is output naar terminal STDOUT")

    server_args.add_argument("-a", "--chucks_server", required=False, metavar="", default=20, type=int,
                             dest="chunks", help="Number of chunks to split the data in")
    server_args.add_argument("fastq_files", action="store", type=str, nargs='*',
                             help="Minstens 1 Illumina Fastq Format file om te verwerken")

    client_args = argparser.add_argument_group(title="Arguments when run in client mode")
    client_args.add_argument("--cores", "-n", action="store",
                             dest="cores", required=False, type=int,
                             help="Aantal cores om te gebruiken per host.")
    client_args.add_argument("-b", "--chucks_client", required=False, metavar="", default=20,
                             dest="chunks", help="Number of chunks to split the data in")
    client_args.add_argument("--host", action="store", type=str, help="The hostname where the Server is listening")
    client_args.add_argument("--port", action="store", type=int, help="The port on which the Server is listening")

    args = argparser.parse_args()

    # constants
    POISONPILL = "MEMENTOMORI"
    ip_adres = ""
    port = 5019
    AUTHKEY = b'whathasitgotinitspocketsesss?'

    start = time.time()

    if len(args.fastq_files) > 1:
        if args.csvfile is None:
            csv_file = None
        else:
            csv_file = f'{args.fastq_files}.{args.csvfile}'
    else:
        csv_file = args.csvfile

    server = mp.Process(target=runserver, args=(calc_quality_score, args.fastq_files, csv_file, port, POISONPILL))
    server.start()
    time.sleep(1)
    client = mp.Process(target=runclient, args=(args.cores, ip_adres, port, AUTHKEY, POISONPILL))
    client.start()
    server.join()
    client.join()

    end = time.time()

    print(f"Finished in {round(end - start, 2)} seconds")


if __name__ == "__main__":
    sys.exit(main(sys.argv))

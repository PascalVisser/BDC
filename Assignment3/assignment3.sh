#!/bin/bash
#SBATCH --time 2:00:00
#SBATCH --nodes=1
#SBATCH --cpus_per_task=16
#SBATCH --partition=assemblix
export DATA=/data/dataprocessing/MinIONData/all.fq
export REF=/data/dataprocessing/MinIONData/all_bacteria.fna
for n in {1..16} ; do /usr/bin/time -o timings3.txt --append -f "${n}\t%e" Minimap2  -a -t ${n} $REF $DATA > /dev/null ; done
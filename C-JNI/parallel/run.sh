#!/bin/bash
mpirun --allow-run-as-root -n $1 ./mpi-read -f /a -v b -t int -q 'a<b' -n 4

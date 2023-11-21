#!/bin/bash

echo "Using: " $(which om_monitor.py)
mpirun -n {{n_processes}} om_monitor.py {{om_source}} -c {{om_config}} &> batch.out &
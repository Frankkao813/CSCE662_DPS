#!/bin/bash

# need to run this command below
# chmod +x stop.sh

# Stop the coordinator
pkill -f "./coordinator"

# Stop tsd processes
pkill -f "./tsd"

# Stop tsc processes
pkill -f "./tsc"

pkill -f "./synchronizer"

# Clean up semaphores
echo "Cleaning up semaphores..."
rm -f /dev/shm/sem.*

# rm -rf cluster_*

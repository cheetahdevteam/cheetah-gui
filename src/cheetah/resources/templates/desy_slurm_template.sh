#!/bin/bash

echo "Using: " $(which om_monitor.py)

FULLCOMMAND="mpirun om_monitor.py {{om_source}} -c {{om_config}} {{event_list_arg}} "
echo $FULLCOMMAND

sbatch << EOF
#!/bin/bash

#SBATCH -p {{queue}}
#SBATCH -t 10:00:00
#SBATCH --job-name {{job_name}}
#SBATCH --output batch.out
#SBATCH --ntasks={{n_processes}}
$FULLCOMMAND
EOF

echo "Job {{job_name}} sent to queue {{queue}}"
echo ""

#!/bin/bash

echo "Using: " $(which om_monitor)

FULLCOMMAND="mpirun om_monitor {{om_source}} -c {{om_config}} {{event_list_arg}}"
echo $FULLCOMMAND

sbatch << EOF
#!/bin/bash

#SBATCH -p {{queue}}
#SBATCH --account lcls:{{experiment_id}}
#SBATCH -t 10:00:00
#SBATCH --job-name {{job_name}}
#SBATCH --output batch.out
#SBATCH --ntasks=60
$FULLCOMMAND
EOF

echo "Job {{job_name}} sent to queue {{queue}}"
echo ""
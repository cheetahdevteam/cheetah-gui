#!/bin/bash

echo "Using: " $(which om_monitor)

sbatch << EOF
#!/bin/bash

#SBATCH -p {{queue}}
#SBATCH --account lcls:{{experiment_id}}
#SBATCH -t 10:00:00
#SBATCH --job-name {{job_name}}
#SBATCH --output batch.out
#SBATCH --nodes=1
#SBATCH --ntasks-per-node=120

source /sdf/group/lcls/ds/tools/crystfel/setup-crystfel.sh

indexamajig --zmq-input=ipc:///{{output_dir}}/ipc-socket --zmq-request=next  \
	-g {{geometry_file}} -j 80 --peaks=msgpack --copy-header=timestamp --copy-header=event_id \
	--copy-header=source --copy-header=configuration_file --data-format=msgpack \
	{{cell_file_arg}} {{indexing_arg}} {{extra_args}} \
	-o {{filename_prefix}}.stream > crystfel.out 2>&1 &

pid=$!

mpirun -n 40 om_monitor {{om_source}} -c {{om_config}} {{event_list_arg}} > om.out 2>&1

sleep 60
kill -10 $pid
rm -rf indexamajig.*

EOF

echo "Job {{job_name}} sent to queue {{queue}}"
echo ""
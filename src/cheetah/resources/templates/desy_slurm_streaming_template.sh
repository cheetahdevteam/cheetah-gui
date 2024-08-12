#!/bin/bash

echo "Using: " $(which om_monitor.py)

sbatch << EOF
#!/bin/bash

#SBATCH -p {{queue}}
#SBATCH -t 10:00:00
#SBATCH --job-name {{job_name}}
#SBATCH --output batch.out

module load maxwell crystfel/0-devel

indexamajig --zmq-input=ipc:///{{output_dir}}/ipc-socket --zmq-request=next  \
	-g {{geometry_file}} -j 16 --peaks=msgpack --copy-header=timestamp --copy-header=event_id \
	--copy-header=source --copy-header=configuration_file --data-format=msgpack \
	{{cell_file_arg}} {{indexing_arg}} {{extra_args}} \
	-o {{filename_prefix}}.stream > crystfel.out 2>&1 &

pid=$!

mpirun -n 16 om_monitor.py {{om_source}} -c {{om_config}} {{event_list_arg}} > om.out 2>&1

sleep 60
kill -10 $pid
rm -rf indexamajig.*

EOF

echo "Job {{job_name}} sent to queue {{queue}}"
echo ""
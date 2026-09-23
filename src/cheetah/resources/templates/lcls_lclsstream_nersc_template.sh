#!/bin/bash

geometry_filename=$(basename {{geometry_file}})

if [ -z "{{mask_file}}" ]; then
	mask_filename=""
else
	mask_filename=$(basename {{mask_file}})
	# Change mask_file in {{geometry_file}} to mask_filename
	sed -i "s|mask_file.*|mask_file = $mask_filename|" {{geometry_file}}
fi

if [ -z "{{cell_file}}" ]; then
	cell_arg=""
else
	cell_arg="-p $(basename {{cell_file}})"
fi
# TODO: Copy {{geometry_file}}, {{mask_file}} and {{cell_file}} to nersc


uv run submit_crystfel_nersc.py --exp {{experiment_id}} --run {{run_id}} --status <job_id> \
--crystfel-args -g {{geometry_file}} --peaks peakfinder8 --threshold 10 --min-snr 6 \
${cell_arg} {{extra_args}} -o {{filename_prefix}}.stream

# TODO: Copy stream file to {{output_dir}}

echo "Job {{job_name}} sent to nersc"
echo ""
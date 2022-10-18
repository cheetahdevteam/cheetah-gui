#!/usr/bin/env python

import click  # type: ignore
import pathlib
import shutil
import subprocess
import yaml

from typing import List, TextIO, Dict, Any

from cheetah.experiment import CheetahExperiment
from cheetah.crawlers import Crawler
from cheetah.utils.yaml_dumper import CheetahSafeDumper


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument(  # type: ignore
    "runs",
    nargs=-1,
    type=str,
    required=True,
    metavar="RUN_ID(S)",
)
@click.option(  # type: ignore
    "--config-template",
    "-c",
    "config_template",
    nargs=1,
    type=click.Path(exists=True),
    default="template.yaml",
    help="OM config template file, default: ../process/template.yaml",
)
@click.option(  # type: ignore
    "--experiment-config",
    "-e",
    "experiment_config",
    nargs=1,
    type=click.Path(exists=True),
    default="../gui/crawler.config",
    help="experiment crawler.config file, default: ../gui/crawler.config",
)
@click.option(  # type: ignore
    "--tag", "-t", "tag", type=str, default="", help="dataset tag, default: ''"
)
@click.option(  # type: ignore
    "--copy-templates",
    is_flag=True,
    default=False,
    help=(
        "copy resulting template files to the same directory as input template, "
        "default: False"
    ),
)
def main(
    runs: List[str],
    config_template: str,
    experiment_config: str,
    tag: str,
    copy_templates: bool,
) -> None:
    """
    Process JUNGFRAU 1M darks. This script runs om_jungfrau_dark.py script on a list of
    dark runs. The runs are specified by their RUN_ID(S) in the same format as they are
    displayed in the Cheetah GUI table. In addition to the list of run IDs the script
    requires process OM config template ('--config-template' / '-c') and Cheetah
    crawler.config file ('--experiment-config' / '-e'). These files are picked
    automatically when the script is started from the experiment cheetah/calib
    directory. Resulting dark calibration files and OM config template are saved in the
    corresponding run directory in cheetah/hdf5. Additionally, resulting config temlate
    files can be copied to the same directory as input template if '--copy-template'
    option is used.
    """
    experiment: CheetahExperiment = CheetahExperiment(
        pathlib.Path(experiment_config).parent, gui=False
    )
    fh: TextIO
    with open(config_template, "r") as fh:
        config: Dict[str, Any] = yaml.safe_load(fh.read())
    crawler: Crawler = experiment.start_crawler()
    run_id: str
    output_directories: Dict[str, pathlib.Path] = {}
    output_template_names: Dict[str, str] = {}
    for run_id in runs:
        # First, iterate through runs, create or clean output directories and write
        # 'Not finished' status to status.txt files
        raw_id: str = crawler.table_id_to_raw_id(run_id)
        proc_id: str = crawler.raw_id_to_proc_id(raw_id)
        if tag:
            output_directory_name: str = f"{proc_id}-{tag}"
        else:
            output_directory_name = proc_id
        output_directory: pathlib.Path = (
            experiment.get_proc_directory() / output_directory_name
        )
        if output_directory.is_dir():
            print(
                f"Moving to existing data directory {output_directory}\n"
                f"Deleting previous files"
            )
            shutil.rmtree(output_directory)
        else:
            print(f"Creating hdf5 dark directory {output_directory}")
        output_directory.mkdir(parents=True)

        with open(output_directory / "status.txt", "w") as fh:
            fh.write("Status: Not finished\n")

        output_directories[raw_id] = output_directory
        output_template_names[
            raw_id
        ] = f"template-{output_directory_name.replace('/', '_')}.yaml"

    for raw_id, output_directory in output_directories.items():
        # Second, iterate through runs and create darks, write config template files
        # with new darks and change status to 'Dark ready' when finished.
        data_directory: pathlib.Path = (experiment.get_raw_directory() / raw_id).parent
        filename_prefix: str = raw_id.split("/")[-1]
        i: int
        module: str
        for i, module in enumerate(("d0", "d1")):
            list_filename: pathlib.Path = output_directory / f"dark_{module}.lst"
            dark_filename: pathlib.Path = output_directory / f"dark_{module}.h5"
            with open(list_filename, "w") as fh:
                fh.writelines(
                    (
                        f"{data_directory/filename}\n"
                        for filename in sorted(
                            data_directory.glob(f"{filename_prefix}_*_{module}_*.h5")
                        )
                    )
                )
            command: str = f"om_jungfrau_dark.py {list_filename} {dark_filename}"
            print(command)
            result: subprocess.CompletedProcess[bytes] = subprocess.run(
                command, shell=True, capture_output=True
            )
            print(result.stdout.decode("utf-8"))
            print(result.stderr.decode("utf-8"))

            config["data_retrieval_layer"]["detector_dark_filenames"][i] = dark_filename

        output_template: pathlib.Path = (
            output_directory / pathlib.Path(config_template).name
        )
        print(f"Writing OM config template to {output_template}.")
        with open(output_template, "w") as fh:
            fh.write(yaml.dump(config, Dumper=CheetahSafeDumper, sort_keys=False))

        if copy_templates:
            new_template: pathlib.Path = (
                pathlib.Path(config_template).parent / output_template_names[raw_id]
            )
            print(f"Copying new temlate to {new_template}.")
            shutil.copyfile(output_template, new_template)

        with open(output_directory / "status.txt", "w") as fh:
            fh.write("Status: Dark ready\n")


if __name__ == "__main__":
    main()

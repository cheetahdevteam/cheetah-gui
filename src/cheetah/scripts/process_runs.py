"""
Process script.

This module contains Cheetah process script.
"""

import click  # type: ignore
import pathlib

from typing import Union, List

from cheetah.experiment import CheetahExperiment
from cheetah.process import CheetahProcess, TypeProcessingConfig


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument(  # type: ignore
    "runs",
    nargs=-1,
    type=str,
    required=True,
    metavar="RUN_ID(S)",
)
@click.option(  # type: ignore
    "--process-template",
    "-p",
    "process_template",
    nargs=1,
    type=click.Path(exists=True),
    default="process_template.sh",
    help="process template script, default: process_template.sh",
)
@click.option(  # type: ignore
    "--config-template",
    "-c",
    "config_template",
    nargs=1,
    type=click.Path(exists=True),
    default="template.yaml",
    help="OM config template file, default: template.yaml",
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
    "--geometry",
    "-g",
    "geometry_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
    help="CrystFEL geometry file, default: taken from the experiment config",
)
@click.option(  # type: ignore
    "--mask",
    "-m",
    "mask_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
    help="mask HDF5 file, default: taken from the experiment config",
)
@click.option(  # type: ignore
    "--tag", "-t", "tag", type=str, default="", help="dataset tag, default: ''"
)
@click.option(  # type: ignore
    "--queue",
    "-q",
    "queue",
    type=str,
    help="batch queue name, default: guessed based on location",
)
@click.option(  # type: ignore
    "--n-processes",
    "-n",
    "n_processes",
    type=int,
    help="number of OM processes, default: 12",
)
def main(
    runs: List[str],
    process_template: str,
    config_template: str,
    experiment_config: str,
    geometry_filename: Union[str, None],
    mask_filename: Union[str, None],
    tag: str,
    queue: Union[str, None],
    n_processes: Union[int, None],
) -> None:
    """
    Cheetah Processing. This script launches processing of one or several runs. The runs
    are specified by their RUN_ID(S) in the same format as they are displayed in the
    Cheetah GUI table. In addition to the list of run IDs the script requires process
    script template ('-process-template' / '-p'), OM config template
    ('--config-template' / '-c') and Cheetah crawler.config file ('--experiment-config'
    / '-e'). These files are picked automatically when the script is started from the
    experiment cheetah/process directory.
    """
    experiment: CheetahExperiment = CheetahExperiment(
        pathlib.Path(experiment_config).parent, gui=False
    )
    process: CheetahProcess = CheetahProcess(
        experiment.get_facility(),
        experiment.get_instrument(),
        experiment.get_detector(),
        experiment.get_id(),
        pathlib.Path(process_template),
        experiment.get_raw_directory(),
        experiment.get_proc_directory(),
    )
    config: TypeProcessingConfig = experiment.get_last_processing_config()
    config["tag"] = tag
    config["config_template"] = config_template
    if geometry_filename:
        config["geometry"] = str(pathlib.Path(geometry_filename).absolute())
    if mask_filename:
        config["mask"] = str(pathlib.Path(mask_filename).absolute())

    run: str
    for run in runs:
        process.process_run(
            experiment.crawler_table_id_to_raw_id(run), config, queue, n_processes
        )


if __name__ == "__main__":
    main()

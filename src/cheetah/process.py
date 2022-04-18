"""
Cheetah Process.

This module contains classes and functions that allow configuring and launching Cheetah
processing jobs.
"""
import click  # type: ignore
import jinja2
import pathlib
import shutil
import stat
import subprocess
import yaml

from typing import Callable, TextIO, Union

try:
    from typing import Literal, TypedDict
except:
    from typing_extensions import Literal, TypedDict  # type: ignore

from cheetah.crawlers import facilities
from cheetah.utils.yaml_dumper import CheetahSafeDumper

class _TypeOmConfigTemplateData(TypedDict, total=False):
    # A dictionary used internally to store information required to fill OM config
    # template, which can be used to process data from a single run.

    psana_calib_dir: pathlib.Path
    output_dir: pathlib.Path
    experiment_id: str
    run_id: str
    filename_prefix: str
    geometry_file: pathlib.Path
    mask_file: Union[pathlib.Path, Literal["null"]]


class _TypeProcessScriptTemplateData(TypedDict):
    # A dictionary used internally to store information required to fill process script
    # template, which can be used to process data from a single run.

    queue: str
    job_name: str
    n_processes: int
    om_source: str
    om_config: pathlib.Path


class TypeProcessingConfig(TypedDict):
    """
    A dictionary storing processing configuration parameters.

    Attributes:

        config_template: The path of OM config template file.

        tag: The dataset tag.

        geometry: The path of the geometry file.

        mask: The path of the mask file.
    """

    config_template: str
    tag: str
    geometry: str
    mask: str


class CheetahProcess:
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        facility: str,
        experiment_id: str,
        process_template: pathlib.Path,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
    ) -> None:
        """
        Cheetah Process.

        This class stores all the parameters needed to process data from a particular
        experiment using Cheetah processing layer in [OM][https://ondamonitor.com]. It
        can then launch a processing job using provided run ID and processing config on
        request.

        Arguments:

            facility: The name of the facility.

            experiment_id: Experiment ID.

            process_template: The path of the processing script template (usually
                cheetah/process/process_template.sh).

            raw_directory: The path of the raw data directory.

            proc_directory: The path of the processed data directory.
        """
        self._facility: str = facility
        self._experiment_id: str = experiment_id
        self._process_template_file: pathlib.Path = process_template
        fh: TextIO
        with open(self._process_template_file) as fh:
            self._process_template: jinja2.Template = jinja2.Template(fh.read())
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._prepare_om_source: Callable[
            [str, str, pathlib.Path, pathlib.Path], str
        ] = facilities[self._facility]["prepare_om_source"]

    def _raw_id_to_proc_id(self, raw_id: str) -> str:
        # Converts raw run ID to processed run ID by replacing all "-" signs with "_".
        return raw_id.replace("-", "_")

    def _write_process_config_file(
        self,
        output_directory: pathlib.Path,
        config: TypeProcessingConfig,
        process_template_data: _TypeProcessScriptTemplateData,
        om_config_template_data: _TypeOmConfigTemplateData,
    ) -> None:
        # Writes process.config file in the output run directory.
        fh: TextIO
        with open(output_directory / "process.config", "w") as fh:
            fh.write(
                yaml.dump(
                    {
                        "Processing config": config,
                        "Process script template data": process_template_data,
                        "OM config template data": om_config_template_data,
                    },
                    Dumper=CheetahSafeDumper,
                    sort_keys=False,
                )
            )

    def _write_status_file(self, output_directory: pathlib.Path) -> None:
        # Writes status.txt file after submitting the job.
        fh: TextIO
        with open(output_directory / "status.txt", "w") as fh:
            fh.write("# Cheetah status\n")
            fh.write("Status: Submitted\n")

    def process_run(
        self,
        run_id: str,
        config: TypeProcessingConfig,
        queue: Union[str, None] = None,
        n_processes: Union[int, None] = None,
    ) -> None:
        """
        Launch processing of a single run.

        This function launches data processing of a single run. First, it either
        creates a new run directory or deletes all previous files if the directory
        already exists. The name of the output directory consists of the run ID (where
        all "-" signs are replaced by "_") and the dataset tag, separated by "-". Then
        it fills process script template and OM config template with the experiment
        data and provided configuration parameters and saves them in the output run
        directory. The resulting script is then started in a separate process.

        Arguments:

            run_id: Run ID of the raw data.

            config: A [TypeProcessingConfig][cheetah.process.TypeProcessingConfig]
                dictionary containing processing configuration parameteres.

            queue: The name of the batch queue where the processing job should be
                submitted. This parameter will be inserted in the process script
                template. If the value of this parameter is None the facility-specific
                geuss_batch_queue function (defined in
                [facilities][cheetah.crawlers.facilities]) will be used to guess the
                appropriate queue name. Defaults to None.

            n_processes: The number of nodes OM should use for processing. This
                parameter will be inserted in the process script template. If this
                parameter is None, 12 nodes will be used. Defaults to None.
        """
        om_config_template_file: pathlib.Path = pathlib.Path(config["config_template"])
        tag: str = config["tag"]
        geometry_file: pathlib.Path = pathlib.Path(config["geometry"])
        if config["mask"]:
            mask_file: Union[pathlib.Path, Literal["null"]] = pathlib.Path(
                config["mask"]
            )
        else:
            mask_file = "null"

        proc_id: str = self._raw_id_to_proc_id(run_id)
        if tag:
            output_directory_name: str = f"{proc_id}-{tag}"
        else:
            output_directory_name = proc_id
        output_directory: pathlib.Path = self._proc_directory / output_directory_name

        if output_directory.is_dir():
            print(
                f"Moving to existing data directory {output_directory}\n"
                f"Deleting previous files"
            )
            shutil.rmtree(output_directory)
        else:
            print(f"Creating hdf5 data directory {output_directory}")
        output_directory.mkdir(parents=True)

        print(f"Copying configuration file: {om_config_template_file}")
        om_config_file: pathlib.Path = output_directory / "monitor.yaml"

        fh: TextIO
        with open(om_config_template_file) as fh:
            om_config_template: jinja2.Template = jinja2.Template(fh.read())

        om_config_data: _TypeOmConfigTemplateData = {
            "psana_calib_dir": self._raw_directory.parent / "calib",
            "filename_prefix": proc_id.split("/")[-1],
            "output_dir": output_directory,
            "experiment_id": self._experiment_id,
            "run_id": proc_id,
            "geometry_file": geometry_file,
            "mask_file": mask_file,
        }
        with open(om_config_file, "w") as fh:
            fh.write(om_config_template.render(om_config_data))

        process_script: pathlib.Path = output_directory / "process.sh"
        om_source: str = self._prepare_om_source(
            run_id, self._experiment_id, self._raw_directory, output_directory
        )

        if not queue:
            queue = facilities[self._facility]["guess_batch_queue"](self._raw_directory)
        if not n_processes:
            n_processes = 12
        process_script_data: _TypeProcessScriptTemplateData = {
            "queue": queue,
            "job_name": output_directory_name,
            "n_processes": n_processes,
            "om_source": om_source,
            "om_config": om_config_file,
        }
        with open(process_script, "w") as fh:
            fh.write(self._process_template.render(process_script_data))

        process_script.chmod(process_script.stat().st_mode | stat.S_IEXEC)
        subprocess.run(f"{process_script}", cwd=output_directory)
        self._write_status_file(output_directory)
        self._write_process_config_file(
            output_directory, config, process_script_data, om_config_data
        )


@click.command()  # type: ignore
def main() -> None:
    """ """
    pass
    # TODO: make cheetah_process.py a standalone script


if __name__ == "__main__":
    main()

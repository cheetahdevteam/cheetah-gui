"""
Cheetah Process.

This module contains classes and functions that allow configuring and launching Cheetah
processing jobs.
"""
import pathlib
import shutil
import stat
import subprocess
from typing import Any, Callable, Dict, TextIO, Union

import jinja2
import yaml

try:
    from typing import Literal, TypedDict
except:
    from typing_extensions import Literal, TypedDict  # type: ignore

from cheetah.crawlers import facilities
from cheetah.utils.yaml_dumper import CheetahSafeDumper


class _TypeOmConfigTemplateData(TypedDict, total=False):
    # A dictionary used internally to store information required to fill OM config
    # template, which can be used to process data from a single run.

    processing_layer: Union[
        Literal["CheetahProcessing"], Literal["StreamingCheetahProcessing"]
    ]
    psana_calib_dir: pathlib.Path
    output_dir: pathlib.Path
    experiment_id: str
    run_id: str
    filename_prefix: str
    geometry_file: pathlib.Path
    mask_file: Union[pathlib.Path, Literal["null"]]


class _TypeProcessScriptTemplateData(TypedDict, total=False):
    # A dictionary used internally to store information required to fill process script
    # template, which can be used to process data from a single run.

    queue: str
    job_name: str
    n_processes: int
    om_source: str
    om_config: pathlib.Path
    filename_prefix: str
    geometry_file: pathlib.Path
    output_dir: pathlib.Path


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
        instrument: str,
        detector: str,
        experiment_id: str,
        process_template: pathlib.Path,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
        streaming: bool = False,
    ) -> None:
        """
        Cheetah Process.

        This class stores all the parameters needed to process data from a particular
        experiment using Cheetah processing layer in [OM][https://ondamonitor.com]. It
        can then launch a processing job using provided run ID and processing config on
        request.

        Arguments:

            facility: The name of the facility.

            instrument: The name of the instrument.

            detector: The name of the detector.

            experiment_id: Experiment ID.

            process_template: The path of the processing script template (usually
                cheetah/process/process_template.sh).

            raw_directory: The path of the raw data directory.

            proc_directory: The path of the processed data directory.
        """
        self._facility: str = facility
        self._experiment_id: str = experiment_id
        self._process_template_file: pathlib.Path = process_template
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._prepare_om_source: Callable[
            [str, str, pathlib.Path, pathlib.Path], str
        ] = facilities[self._facility]["instruments"][instrument]["detectors"][
            detector
        ][
            "prepare_om_source"
        ]
        self._kill_processing_job: Callable[[str], str] = facilities[self._facility][
            "kill_processing_job"
        ]
        if streaming:
            self._om_processing_layer: Union[
                Literal["CheetahProcessing"],
                Literal["StreamingCheetahProcessing"],
            ] = "StreamingCheetahProcessing"
        else:
            self._om_processing_layer = "CheetahProcessing"

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

    def _write_status_file(
        self, filename: pathlib.Path, status: Dict[str, Any]
    ) -> None:
        # Writes status.txt file after submitting the job.
        fh: TextIO
        with open(filename, "w") as fh:
            fh.write("# Cheetah status\n")
            fh.write(yaml.dump(status, Dumper=CheetahSafeDumper, sort_keys=False))

    def kill_processing(self, run_proc_dir: str) -> str:
        """
        Kill run processing job.

        This function tries to kill the processing of the provided run using
        facility-specific kill_processing_job function (defined in
        [facilities][cheetah.crawlers.facilities]).

        Arguments:

            run_proc_dir: The path of the processed run directory relative to the
                experiment proc directory (as displayed in the Cheetah GUI table).

        Returns:

            Either an empty string if the job was killed successfully or the error
            message.
        """
        status_filename: pathlib.Path = (
            self._proc_directory / run_proc_dir / "status.txt"
        )
        if status_filename.is_file():
            fh: TextIO
            with open(status_filename, "r") as fh:
                status: Dict[str, Any] = yaml.safe_load(fh.read())
        else:
            return f"Job directory {run_proc_dir} doesn't exist."

        if status["Status"] == "Finished":
            return f"Processing job {run_proc_dir} is already finished."

        error: str = self._kill_processing_job(run_proc_dir)
        if error == "":
            status["Status"] = "Cancelled"
            self._write_status_file(status_filename, status)
        else:
            print(error)
        return error

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
        proc_id: str = self._raw_id_to_proc_id(run_id)
        tag: str = config["tag"]
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
            try:
                shutil.rmtree(output_directory)
            except Exception as e:
                print(f"Couldn't clean {output_directory}: {e}.")
                return
        else:
            print(f"Creating hdf5 data directory {output_directory}")
        output_directory.mkdir(parents=True)

        # Copy input files to the output directory
        input_files_directory: pathlib.Path = output_directory / "input_files"
        input_files_directory.mkdir()

        om_config_template_file: pathlib.Path = (
            input_files_directory / pathlib.Path(config["config_template"]).name
        )
        shutil.copy(config["config_template"], om_config_template_file)

        geometry_file: pathlib.Path = (
            input_files_directory / pathlib.Path(config["geometry"]).name
        )
        shutil.copy(config["geometry"], geometry_file)

        if config["mask"]:
            mask_file: Union[pathlib.Path, Literal["null"]] = (
                input_files_directory / pathlib.Path(config["mask"]).name
            )
            shutil.copy(config["mask"], mask_file)
        else:
            mask_file = "null"

        print(f"Copying configuration file: {om_config_template_file}")
        om_config_file: pathlib.Path = output_directory / "monitor.yaml"

        fh: TextIO
        with open(om_config_template_file) as fh:
            om_config_template: jinja2.Template = jinja2.Template(fh.read())

        om_config_data: _TypeOmConfigTemplateData = {
            "processing_layer": self._om_processing_layer,
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
        with open(self._process_template_file) as fh:
            process_template: jinja2.Template = jinja2.Template(fh.read())
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
            "filename_prefix": proc_id.split("/")[-1],
            "output_dir": output_directory,
            "geometry_file": geometry_file,
        }
        with open(process_script, "w") as fh:
            fh.write(process_template.render(process_script_data))

        process_script.chmod(process_script.stat().st_mode | stat.S_IEXEC)
        subprocess.run(f"{process_script}", cwd=output_directory)
        self._write_status_file(
            output_directory / "status.txt", {"Status": "Submitted"}
        )
        self._write_process_config_file(
            output_directory, config, process_script_data, om_config_data
        )

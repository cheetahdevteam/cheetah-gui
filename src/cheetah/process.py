"""
Cheetah Process.

This module contains classes and functions that allow configuring and launching Cheetah
processing jobs.
"""

import copy
import logging
import pathlib
import shutil
import stat
import subprocess
import time
from dataclasses import asdict, dataclass
from typing import Any, Callable, Dict, Optional, TextIO, Union

import jinja2
import yaml
from typing_extensions import Literal

from cheetah.crawlers import facilities
from cheetah.utils.logging import log_subprocess_run_output
from cheetah.utils.yaml_dumper import CheetahSafeDumper

logger = logging.getLogger(__name__)


@dataclass
class _OmConfigTemplateData:
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


@dataclass
class _ProcessScriptTemplateData:
    # A dictionary used internally to store information required to fill process script
    # template, which can be used to process data from a single run.

    queue: str
    job_name: str
    n_processes: int
    om_source: str
    om_config: pathlib.Path
    event_list_arg: str
    filename_prefix: str
    geometry_file: pathlib.Path
    output_dir: pathlib.Path
    experiment_id: str
    cell_file_arg: str
    indexing_arg: str
    extra_args: str


@dataclass
class IndexingConfig:
    """
    A dictionary storing indexing configuration parameters.

    Attributes:

        cell_file: The path of the cell file passed to indexamajig `-p` argument.

        indexing: Indexing methods passed to indexamajig `--indexing` argument.

        extra_args: Extra arguments passed to the indexamajig command.
    """

    cell_file: str
    indexing: str
    extra_args: str


@dataclass
class ProcessingConfig:
    """
    A dictionary storing processing configuration parameters.

    Attributes:

        config_template: The path of OM config template file.

        tag: The dataset tag.

        geometry: The path of the geometry file.

        mask: The path of the mask file.

        indexing_config: A [TypeIndexingConfig][cheetah.process.TypeIndexingConfig]
            dictionary containing indexing configuration parameters or None if indexing
            is not used.

        event_list: The path of the event list file or None if all events should be
            processed.

        write_data_files: A boolean value indicating whether data HDF5 files should be
            written.
    """

    config_template: str
    tag: str
    geometry: str
    mask: str
    indexing_config: Optional[IndexingConfig]
    event_list: Optional[str]
    write_data_files: bool


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
        ] = (
            facilities[self._facility]
            .instruments[instrument]
            .detectors[detector]
            .prepare_om_source
        )
        self._kill_processing_job: Callable[[str, pathlib.Path], str] = facilities[
            self._facility
        ].kill_processing_job
        if streaming:
            self._om_processing_layer: Union[
                Literal["CheetahProcessing"],
                Literal["StreamingCheetahProcessing"],
            ] = "StreamingCheetahProcessing"
        else:
            self._om_processing_layer = "CheetahProcessing"

    def _raw_id_to_proc_id(self, raw_id: str) -> str:
        # Converts raw run ID to processed run ID by replacing all "-" signs with "_".
        # Also adds "_" at the beginning of the string if it starts with ".".
        proc_id: str = raw_id.replace("-", "_")
        if proc_id[0] == ".":
            proc_id = "_" + proc_id[1:]
        return proc_id

    def _write_process_config_file(
        self,
        output_directory: pathlib.Path,
        config: ProcessingConfig,
        process_template_data: _ProcessScriptTemplateData,
        om_config_template_data: _OmConfigTemplateData,
    ) -> None:
        # Writes process.config file in the output run directory.
        fh: TextIO
        with open(output_directory / "process.config", "w") as fh:
            fh.write(
                yaml.dump(
                    {
                        "Processing config": asdict(config),
                        "Process script template data": asdict(process_template_data),
                        "OM config template data": asdict(om_config_template_data),
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

        error: str = self._kill_processing_job(
            run_proc_dir, self._proc_directory / run_proc_dir
        )
        if error == "":
            status["Status"] = "Cancelled"
            self._write_status_file(status_filename, status)
        else:
            logger.error(error)
        return error

    def _copy_config_files(self, config: ProcessingConfig, directory: pathlib.Path):
        # Copy configuration files to the output directory.
        directory.mkdir(parents=True, exist_ok=True)
        config_dict = asdict(config)
        new_config_dict = config_dict.copy()
        for key in ["config_template", "geometry", "mask", "event_list"]:
            if not config_dict[key]:
                continue
            filename: pathlib.Path = directory / pathlib.Path(config_dict[key]).name
            shutil.copy(config_dict[key], filename)
            new_config_dict[key] = str(filename)
        config = ProcessingConfig(**new_config_dict)

        if config.indexing_config:
            if config.indexing_config.cell_file:
                filename: pathlib.Path = (
                    directory / pathlib.Path(config.indexing_config.cell_file).name
                )
                shutil.copy(config.indexing_config.cell_file, filename)
                config.indexing_config.cell_file = str(filename)

    def _setup_output_directory(
        self, directory: pathlib.Path, config: ProcessingConfig
    ) -> Optional[pathlib.Path]:
        # Create output directory for the processed run.

        # Copy input files to a temporary directory
        temp_directory: pathlib.Path = self._proc_directory / f"temp_{time.time_ns()}"
        self._copy_config_files(config, temp_directory)

        if directory.is_dir():
            logger.info(
                f"Moving to existing data directory {directory}\n"
                f"Deleting previous files"
            )
            try:
                shutil.rmtree(directory)
            except Exception as e:
                logger.error(f"Couldn't clean {directory}: {e}.")
                return None
        else:
            logger.info(f"Creating hdf5 data directory {directory}")
        directory.mkdir(parents=True)

        # Copy input files to the output directory
        input_files_directory: pathlib.Path = directory / "input_files"
        self._copy_config_files(config, input_files_directory)
        shutil.rmtree(temp_directory)

        return directory

    def process_run(
        self,
        run_id: str,
        config: ProcessingConfig,
        queue: Optional[str] = None,
        n_processes: Optional[int] = None,
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
        input_config: ProcessingConfig = copy.deepcopy(config)
        proc_id: str = self._raw_id_to_proc_id(run_id)
        tag: str = config.tag
        if tag:
            output_directory_name: str = f"{proc_id}-{tag}"
        else:
            output_directory_name = proc_id
        output_directory: Optional[pathlib.Path] = self._setup_output_directory(
            self._proc_directory / output_directory_name, config
        )
        if not output_directory:
            return

        om_config_template_file: pathlib.Path = pathlib.Path(config.config_template)
        geometry_file: pathlib.Path = pathlib.Path(config.geometry)
        mask_file: Union[pathlib.Path, Literal["null"]] = (
            pathlib.Path(config.mask) if config.mask else "null"
        )

        logger.info(f"Copying configuration file: {om_config_template_file}")
        om_config_file: pathlib.Path = output_directory / "monitor.yaml"

        # Fill OM config template with the provided data
        fh: TextIO
        with open(om_config_template_file) as fh:
            om_config_template: jinja2.Template = jinja2.Template(fh.read())

        om_config_data: _OmConfigTemplateData = _OmConfigTemplateData(
            processing_layer=self._om_processing_layer,
            psana_calib_dir=self._raw_directory.parent / "calib",
            filename_prefix=proc_id.split("/")[-1],
            output_dir=output_directory,
            experiment_id=self._experiment_id,
            run_id=proc_id,
            geometry_file=geometry_file,
            mask_file=mask_file,
        )
        with open(om_config_file, "w") as fh:
            fh.write(om_config_template.render(asdict(om_config_data)))

        # If data files are not written, remove the HDF5 fields from the OM config file
        if not config.write_data_files:
            with open(om_config_file, "r") as fh:
                om_config: Dict[str, Any] = yaml.safe_load(fh)
            try:
                om_config["cheetah"]["hdf5_fields"] = {}
                with open(om_config_file, "w") as fh:
                    fh.write(yaml.safe_dump(om_config))
            except KeyError:
                pass

        # Fill process script template with the provided data
        process_script: pathlib.Path = output_directory / "process.sh"
        with open(self._process_template_file) as fh:
            process_template: jinja2.Template = jinja2.Template(fh.read())

        om_source: str = self._prepare_om_source(
            run_id, self._experiment_id, self._raw_directory, output_directory
        )
        if not queue:
            queue = facilities[self._facility].guess_batch_queue(self._raw_directory)
        if not n_processes:
            n_processes = 12

        event_list_arg = ""
        if config.event_list:
            event_list_arg = f"--event-list={config.event_list}"

        cell_file_arg = ""
        indexing_arg = ""
        extra_args = ""
        if config.indexing_config:
            if config.indexing_config.cell_file:
                cell_file_arg: str = f"-p {config.indexing_config.cell_file}"
            if config.indexing_config.indexing:
                indexing_arg: str = f"--indexing={config.indexing_config.indexing}"
            extra_args: str = config.indexing_config.extra_args

        process_script_data: _ProcessScriptTemplateData = _ProcessScriptTemplateData(
            queue=queue,
            job_name=output_directory_name,
            n_processes=n_processes,
            om_source=om_source,
            om_config=om_config_file,
            event_list_arg=event_list_arg,
            filename_prefix=proc_id.split("/")[-1],
            output_dir=output_directory,
            experiment_id=self._experiment_id,
            geometry_file=geometry_file,
            cell_file_arg=cell_file_arg,
            indexing_arg=indexing_arg,
            extra_args=extra_args,
        )
        with open(process_script, "w") as fh:
            fh.write(process_template.render(asdict(process_script_data)))
        process_script.chmod(process_script.stat().st_mode | stat.S_IEXEC)

        # Run the process script
        output: subprocess.CompletedProcess = subprocess.run(
            f"{process_script}", cwd=output_directory, shell=True, capture_output=True
        )
        log_subprocess_run_output(output, logger)

        # Write status and process config files
        self._write_status_file(
            output_directory / "status.txt", {"Status": "Submitted"}
        )
        self._write_process_config_file(
            output_directory, input_config, process_script_data, om_config_data
        )

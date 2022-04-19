"""
Cheetah Experiment.

This module contains classes and functions that provide information related to a 
particular experiment and control its data processing.
"""
import pathlib
import shutil
import yaml

from typing import List, Dict, TextIO, Union, Any, cast

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.crawlers import TypeDetectorInfo, facilities
from cheetah.crawlers.base import Crawler
from cheetah.process import CheetahProcess, TypeProcessingConfig
from cheetah.utils.yaml_dumper import CheetahSafeDumper


class TypeExperimentConfig(TypedDict):
    """
    A dictionary storing all information required to set up new Cheetah experiment.

    Attributes:

        facility: The name of the facility.

        instrument: The name of the instrument.

        detector: The name of the detector.

        raw_dir: Raw data directory.

        experiment_id: Experiment ID.

        output_dir: The path to the directory where the new Cheetah experiment
            directory has to be be created.

        cheetah_resources: Cheetah resources directory (usually
            cheetah_source/resources).
    """

    facility: str
    instrument: str
    detector: str
    raw_dir: str
    experiment_id: str
    output_dir: str
    cheetah_resources: str


class CheetahExperiment:
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        path: pathlib.Path,
        new_experiment_config: Union[None, TypeExperimentConfig] = None,
    ) -> None:
        """
        Cheetah Experiment.

        This class stores all information associated with a particular experiment. It
        can either set up new experiment creating new Cheetah directory on disk or load
        old experiment data from already existing Cheetah directory. It creates an
        instance of [Cheetah Crawler][cheetah.crawlers.base.Crawler] class which can
        scan experiment directories and update the run table in Cheetah GUI.
        Additionally, it creates an instance of
        [Cheetah Process][cheetah.process.CheetahProcess] class which can launch
        processing of selected runs on request.

        Arguments:

            path: When loading already existing experiment - the path to existing
                cheetah/gui directory containing crawler.config file. When setting up
                new Cheetah experiment - the path to the current working directory. In
                the latter case `new_experiment_config` must also be provided.

            new_experiment_config: Either a
                [TypeExperimentConfig][cheetah.experiment.TypeExperimentConfig]
                dictionary or None. If the value of this parameter is None `path` must
                point to already existing cheetah/gui directory.
        """
        if new_experiment_config:
            self._setup_new_experiment(new_experiment_config)
        else:
            self._load_existing_experiment(path)
        self._update_previous_experiments_list()
        self._crawler_csv_filename: pathlib.Path = self._gui_directory / "crawler.txt"
        self._crawler: Crawler = facilities[self._facility]["crawler"](
            self._raw_directory,
            self._proc_directory,
            self._crawler_csv_filename,
            self._crawler_scan_raw_dir,
            self._crawler_scan_proc_dir,
        )
        self._cheetah_process: CheetahProcess = CheetahProcess(
            self._facility,
            self._experiment_id,
            self._process_directory / "process_template.sh",
            self._raw_directory,
            self._proc_directory,
        )
        self.write_crawler_config()

    def write_crawler_config(self) -> None:
        """
        Write crawler config file.

        This function writes all experiment and crawler configuration parameters to the
        crawler.config file in cheetah/gui directory.
        """
        fh: TextIO
        with open(self._crawler_config_filename, "w") as fh:
            fh.write(
                yaml.dump(
                    {
                        "facility": self._facility,
                        "instrument": self._instrument,
                        "detector": self._detector,
                        "experiment_id": self._experiment_id,
                        "base_path": self._base_path,
                        "raw_dir": self._raw_directory.relative_to(self._base_path),
                        "hdf5_dir": self._proc_directory.relative_to(self._base_path),
                        "process_dir": self._process_directory.relative_to(
                            self._base_path
                        ),
                        "crawler_scan_raw_dir": self._crawler.raw_directory_scan_is_enabled(),
                        "crawler_scan_proc_dir": self._crawler.proc_directory_scan_is_enabled(),
                        "geometry": self._last_geometry.relative_to(self._base_path),
                        "mask": self._last_mask.relative_to(self._base_path)
                        if self._last_mask
                        else "",
                        "cheetah_config": self._last_process_config_filename.relative_to(
                            self._base_path
                        ),
                        "cheetah_tag": self._last_tag,
                    },
                    Dumper=CheetahSafeDumper,
                    sort_keys=False,
                )
            )

    def _resolve_path(
        self, path: pathlib.Path, parent_path: pathlib.Path
    ) -> pathlib.Path:
        # Resolves path with respect to parent_path and returns the absolute path.
        if path.is_absolute():
            return path
        # Hack to not resolve links at psana:
        # since raw, calib and scratch directories on psana are often links pointing
        # to different sources on different machines, one has to keep the paths as
        # links instead of resolving them.
        cwd = pathlib.Path.cwd()
        if parent_path in cwd.parents:
            return (parent_path / path).resolve()
        else:
            return parent_path / path

    def _load_existing_experiment(self, path: pathlib.Path) -> None:
        # Loads information from crawler.config file. `path` must point to the existing
        # cheetah/gui directory containing crawler.config file.
        self._gui_directory: pathlib.Path = self._resolve_path(path, pathlib.Path.cwd())
        self._crawler_config_filename: pathlib.Path = (
            self._gui_directory / "crawler.config"
        )
        print(
            f"Going to selected experiment: {self._gui_directory}\n"
            f"Loading configuration file: {self._crawler_config_filename}"
        )
        fh: TextIO
        with open(self._crawler_config_filename, "r") as fh:
            crawler_config: Dict[str, str] = yaml.safe_load(fh.read())

        self._facility = crawler_config["facility"]
        self._instrument = crawler_config["instrument"]
        self._detector = crawler_config["detector"]
        self._experiment_id = crawler_config["experiment_id"]

        self._base_path = pathlib.Path(crawler_config["base_path"])
        self._raw_directory = self._resolve_path(
            pathlib.Path(crawler_config["raw_dir"]), self._base_path
        )
        self._proc_directory = self._resolve_path(
            pathlib.Path(crawler_config["hdf5_dir"]), self._base_path
        )
        self._process_directory = self._resolve_path(
            pathlib.Path(crawler_config["process_dir"]), self._base_path
        )
        self._calib_directory = self._gui_directory.parent / "calib"

        self._crawler_scan_raw_dir: bool = crawler_config["crawler_scan_raw_dir"]
        self._crawler_scan_proc_dir: bool = crawler_config["crawler_scan_proc_dir"]

        self._last_process_config_filename = self._resolve_path(
            pathlib.Path(crawler_config["cheetah_config"]), self._base_path
        )
        self._last_geometry = self._resolve_path(
            pathlib.Path(crawler_config["geometry"]), self._base_path
        )
        if crawler_config["mask"]:
            self._last_mask: Union[pathlib.Path, None] = self._resolve_path(
                pathlib.Path(crawler_config["mask"]), self._base_path
            )
        else:
            self._last_mask = None
        self._last_tag = crawler_config["cheetah_tag"]

    def _setup_new_experiment(
        self, new_experiment_config: TypeExperimentConfig
    ) -> None:
        # Sets up new experiment. Creates new Cheetah directory structure, writes
        # cheetah/gui/crawler.config file and copies required resources to
        # cheetah/calib and cheetah/process.
        print("Setting up new experiment\n")
        self._facility = new_experiment_config["facility"]
        self._instrument = new_experiment_config["instrument"]
        self._detector = new_experiment_config["detector"]
        self._raw_directory = pathlib.Path(new_experiment_config["raw_dir"])
        self._base_path = self._raw_directory.parent
        self._experiment_id = new_experiment_config["experiment_id"]

        print(
            f"Creating new Cheetah directory:\n{new_experiment_config['output_dir']}\n"
        )
        self._gui_directory = pathlib.Path(new_experiment_config["output_dir"]) / "gui"
        self._gui_directory.mkdir(parents=True, exist_ok=False)

        self._proc_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "hdf5"
        )
        self._proc_directory.mkdir(parents=True, exist_ok=False)

        self._calib_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "calib"
        )
        self._calib_directory.mkdir(parents=True, exist_ok=False)

        self._process_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "process"
        )
        self._process_directory.mkdir(parents=True, exist_ok=False)

        self._process_script = pathlib.Path("cheetah_process.py")

        resources: TypeDetectorInfo = facilities[new_experiment_config["facility"]][
            "instruments"
        ][new_experiment_config["instrument"]]["detectors"][
            new_experiment_config["detector"]
        ]
        print(
            f"Copying {new_experiment_config['detector']} geometry and mask to \n"
            f"{self._calib_directory}\n"
        )
        resource: str
        for resource in resources["calib_resources"].values():
            shutil.copyfile(
                pathlib.Path(new_experiment_config["cheetah_resources"]) / resource,
                self._calib_directory / resource,
            )
        self._last_geometry = (
            self._calib_directory / resources["calib_resources"]["geometry"]
        )
        self._last_mask = self._calib_directory / resources["calib_resources"]["mask"]

        print(
            f"Copying OM config and process script templates to \n"
            f"{self._process_directory}\n"
        )
        om_template: str = resources["om_config_template"]
        process_template: str = resources["process_template"]
        shutil.copyfile(
            pathlib.Path(new_experiment_config["cheetah_resources"])
            / "templates"
            / om_template,
            self._process_directory / "template.yaml",
        )
        shutil.copyfile(
            pathlib.Path(new_experiment_config["cheetah_resources"])
            / "templates"
            / process_template,
            self._process_directory / "process_template.sh",
        )

        self._crawler_config_filename = self._gui_directory / "crawler.config"
        self._crawler_scan_raw_dir = True
        self._crawler_scan_proc_dir = True

        self._last_process_config_filename = self._process_directory / "template.yaml"
        self._last_tag = ""

    def _update_previous_experiments_list(self) -> None:
        # Updates the list of experiments in ~/.cheetah-crawler2, setting current
        # experiment as the most recent one.
        logfile_path: pathlib.Path = pathlib.Path.expanduser(
            pathlib.Path("~/.cheetah-crawler2")
        )
        current_experiment: str = str(self._gui_directory) + "\n"
        if logfile_path.is_file():
            fh: TextIO
            with open(logfile_path, "r") as fh:
                previous_experiments: List[str] = fh.readlines()
            if current_experiment in previous_experiments:
                previous_experiments.remove(current_experiment)
            previous_experiments.insert(0, current_experiment)
        else:
            previous_experiments = [
                current_experiment,
            ]
        with open(logfile_path, "w") as fh:
            fh.writelines(previous_experiments)

    def crawler_table_id_to_raw_id(self, table_id: str) -> str:
        """
        Convert raw run ID to table ID.

        This function uses the method implemented by the facility-specific [Cheetah
        Crawler][cheetah.crawlers.base.Crawler] to convert unique identifier of the run
        displayed in the Cheetah GUI run table to the raw data run ID.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        return self._crawler.table_id_to_raw_id(table_id)

    def get_calib_directory(self) -> pathlib.Path:
        """
        Get calib directory.

        This function returns the path of the experiment calib directory.

        Returns:

            The path of the processed data directory.
        """
        return self._calib_directory

    def get_crawler_csv_filename(self) -> pathlib.Path:
        """
        Get the path of the crawler CSV file.

        This function returns the path of the CSV file where Cheetah crawler writes the
        data displayed in the Cheetah GUI run table.

        Returns:

            The path of the crawler CSV file.
        """
        return self._crawler_csv_filename

    def get_last_processing_config(
        self, run_proc_dir: Union[str, None] = None
    ) -> TypeProcessingConfig:
        """
        Get the last processing config.

        This function returns a
        [TypeProcessingConfig][cheetah.process.TypeProcessingConfig] dictionary
        containing configuration of the latest launched processing job either from a
        specified run directory or from the whole experiment.

        Arguments:

            run_proc_dir: Either the path of the processed run directory or None. When
                the value of this parameter is None, the function returns the last
                processing config used for any run in the experiment. Defaults to None.

        Returns:

            The last processing config.
        """
        if run_proc_dir:
            process_config_filename: pathlib.Path = (
                self._proc_directory / run_proc_dir / "process.config"
            )
            if process_config_filename.is_file():
                fh: TextIO
                with open(process_config_filename, "r") as fh:
                    run_process_config: Dict[str, Any] = yaml.safe_load(fh)
                return cast(
                    TypeProcessingConfig, run_process_config["Processing config"]
                )
        return {
            "config_template": str(self._last_process_config_filename),
            "tag": self._last_tag,
            "geometry": str(self._last_geometry),
            "mask": str(self._last_mask) if self._last_mask else "",
        }

    def get_working_directory(self) -> pathlib.Path:
        """
        Get working directory.

        This function returns the path of the Cheetah experiment directory.

        Returns:
            The path of the Cheetah experiment directory.
        """
        return (self._gui_directory / "..").resolve()

    def get_proc_directory(self) -> pathlib.Path:
        """
        Get processed data directory.

        This function returns the path of the directory where processed data is stored.

        Returns:
            The path of the processed data directory.
        """
        return self._proc_directory

    def process_run(
        self,
        run_id: str,
        processing_config: Union[TypeProcessingConfig, None],
        queue: Union[str, None] = None,
        n_processes: Union[int, None] = None,
    ) -> None:
        """
        Launch processing of a single run.

        This function launches processing of a single run calling
        [CheetahProcess.process_run][cheetah.proces.CheetahProcess.process_run] method.

        Arguments:

            run_id: Run ID of the raw data.

            processing_config: Either a
                [TypeProcessingConfig][cheetah.process.TypeProcessingConfig] dictionary
                containing processing configuration parameters or None. If the value of
                this parameter is None the latest used processing configuration will be
                used again.

            queue: The name of the batch queue where the processing job should be
                submitted. This parameter will be passed to
                [CheetahProcess.process_run][cheetah.proces.CheetahProcess.process_run].
                Defaults to None.

            n_processes: The number of nodes OM should use to run data processing. This
                parameter will be passed to
                [CheetahProcess.process_run][cheetah.proces.CheetahProcess.process_run].
                Defaults to None.
        """
        if processing_config is None:
            processing_config = self.get_last_processing_config()
        else:
            self._last_process_config_filename = pathlib.Path(
                processing_config["config_template"]
            )
            self._last_tag = processing_config["tag"]
            self._last_geometry = pathlib.Path(processing_config["geometry"])
            if processing_config["mask"]:
                self._last_mask = pathlib.Path(processing_config["mask"])
            else:
                self._last_mask = None

        self._cheetah_process.process_run(
            run_id,
            processing_config,
            queue,
            n_processes,
        )
        self.write_crawler_config()

    def start_crawler(self) -> Crawler:
        """
        Start Cheetah crawler.

        This function returns an instance of facility-specific
        [Cheetah Crawler][cheetah.crawlers.base.Crawler] created when Experiment was
        initialized.

        Returns:
            Cheetah crawler.
        """
        return self._crawler

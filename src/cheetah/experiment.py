import pathlib
import shutil

from typing import List, Dict, TextIO, TypedDict, Union
from cheetah.crawlers import facilities
from cheetah.crawlers.base import Crawler


class TypeExperimentConfig(TypedDict):
    facility: str
    instrument: str
    detector: str
    raw_dir: str
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
        """ """
        if new_experiment_config:
            self._setup_new_experiment(new_experiment_config)
        else:
            self._load_existing_experiment(path)
        self._update_previous_experiments_list()
        self._crawler_csv_filename: pathlib.Path = self._gui_directory / "crawler.txt"

    def _parse_crawler_config(self) -> Dict[str, str]:
        config: Dict[str, str] = {}
        fh: TextIO
        with open(self._crawler_config_filename, "r") as fh:
            line: str
            for line in fh:
                line_items: List[str] = line.split("=")
                if len(line_items) == 2:
                    config[line_items[0].strip()] = line_items[1].strip()
        return config

    def _write_crawler_config(self) -> None:
        fh: TextIO
        with open(self._crawler_config_filename, "w") as fh:
            fh.write(
                f"facility={self._facility}\n"
                f"instrument={self._instrument}\n"
                f"detector={self._detector}\n"
                f"rawdir={self._raw_directory}\n"
                f"hdf5dir={self._hdf5_directory}\n"
                f"process={self._process_script}\n"
                f"geometry={self._last_geometry}\n"
                f"cheetahini={self._last_process_config_filename}\n"
                f"cheetahtag={self._last_tag}"
            )

    def _resolve_path(
        self, path: pathlib.Path, parent_path: pathlib.Path
    ) -> pathlib.Path:
        if path.is_absolute():
            return path
        else:
            return (parent_path / path).resolve()

    def _load_existing_experiment(self, path: pathlib.Path) -> None:
        self._gui_directory: pathlib.Path = self._resolve_path(path, pathlib.Path.cwd())
        self._crawler_config_filename: pathlib.Path = (
            self._gui_directory / "crawler.config"
        )
        print(
            f"Going to selected experiment: {self._gui_directory}\n"
            f"Loading configuration file: {self._crawler_config_filename}"
        )
        crawler_config: Dict[str, str] = self._parse_crawler_config()
        self._hdf5_directory: pathlib.Path = self._resolve_path(
            pathlib.Path(crawler_config["hdf5dir"]), self._gui_directory
        )
        self._calib_directory: pathlib.Path = (
            self._gui_directory / "../calib"
        ).resolve()
        if "xtcdir" in crawler_config.keys():
            self._raw_directory: pathlib.Path = self._resolve_path(
                pathlib.Path(crawler_config["xtcdir"]), self._gui_directory
            )
            self._facility: str = "LCLS"
            self._instrument: str = ""
            self._detector: str = ""
        else:
            self._raw_directory = self._resolve_path(
                pathlib.Path(crawler_config["rawdir"]), self._gui_directory
            )
            self._facility = crawler_config["facility"]
            self._instrument = crawler_config["instrument"]
            self._detector = crawler_config["detector"]

        self._process_script: pathlib.Path = self._resolve_path(
            pathlib.Path(crawler_config["process"]), self._gui_directory
        )
        self._process_directory: pathlib.Path = self._process_script.parent

        self._last_process_config_filename: Union[
            None, pathlib.Path
        ] = self._resolve_path(
            pathlib.Path(crawler_config["cheetahini"]), self._process_directory
        )
        self._last_geometry: Union[None, pathlib.Path] = self._resolve_path(
            pathlib.Path(crawler_config["geometry"]), self._gui_directory
        )
        self._last_tag: Union[None, str] = crawler_config["cheetahtag"]

    def _setup_new_experiment(
        self, new_experiment_config: TypeExperimentConfig
    ) -> None:
        print("Setting up new experiment\n")
        self._facility = new_experiment_config["facility"]
        self._instrument = new_experiment_config["instrument"]
        self._detector = new_experiment_config["detector"]
        self._raw_directory = pathlib.Path(new_experiment_config["raw_dir"])

        print(
            f"Creating new Cheetah directory:\n{new_experiment_config['output_dir']}\n"
        )
        self._gui_directory = pathlib.Path(new_experiment_config["output_dir"]) / "gui"
        self._gui_directory.mkdir(parents=True, exist_ok=False)

        self._hdf5_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "hdf5"
        )
        self._hdf5_directory.mkdir(parents=True, exist_ok=False)

        self._calib_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "calib"
        )
        self._calib_directory.mkdir(parents=True, exist_ok=False)

        self._process_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "process"
        )
        self._process_directory.mkdir(parents=True, exist_ok=False)

        self._process_script = self._process_directory / "process"

        print(
            f"Copying {new_experiment_config['detector']} geometry and mask to \n"
            f"{self._calib_directory}\n"
        )
        resource: str
        for resource in facilities[new_experiment_config["facility"]]["instruments"][
            new_experiment_config["instrument"]
        ]["detectors"][new_experiment_config["detector"]]["resources"]:
            shutil.copyfile(
                pathlib.Path(new_experiment_config["cheetah_resources"]) / resource,
                self._calib_directory / resource,
            )
        self._crawler_config_filename = self._gui_directory / "crawler.config"

        self._last_process_config_filename = None
        self._last_geometry = None
        self._last_tag = None

        self._write_crawler_config()

    def _update_previous_experiments_list(self) -> None:
        logfile_path: pathlib.Path = pathlib.Path.expanduser(
            pathlib.Path("~/.cheetah-crawler")
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

    def start_crawler(self) -> Crawler:
        crawler: Crawler = facilities[self._facility]["crawler"](
            self._raw_directory,
            self._hdf5_directory,
            self._hdf5_directory,
            self._crawler_csv_filename,
        )
        return crawler

    def get_crawler_csv_filename(self) -> pathlib.Path:
        return self._crawler_csv_filename

    def get_working_directory(self) -> pathlib.Path:
        return (self._gui_directory / "..").resolve()

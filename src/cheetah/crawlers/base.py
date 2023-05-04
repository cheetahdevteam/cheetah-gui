"""
Cheetah Crawler's base classes.

This module contains base classes for Cheetah Crawlers.
"""
import csv
import pathlib
import time
from abc import ABC, abstractmethod
from datetime import datetime
from operator import itemgetter
from typing import Any, Dict, List, TextIO, Tuple, Union, cast

import yaml

try:
    from typing import Literal, TypedDict
except:
    from typing_extensions import Literal, TypedDict  # type: ignore


class TypeProcStatusItem(TypedDict):
    """
    A dictionary storing information about the status of the processing of a single run.

    Attributes:

        run_id: Unique identifier of the corresponding run.

        run_name: The name of the processed directory.

        tag: The dataset tag assigned to the run.

        status: Status of the data processing.

        update_time: The timestamp of the last status update.

        processed: The number of processed images.

        hits: The number of found hits.

        indexed: The number of indexed hits.

        crystals: The number of indexed crystals.

        recipe: The name of the config file used for the data processing.
    """

    run_id: str
    run_name: str
    tag: str
    status: str
    update_time: float
    processed: int
    hits: int
    indexed: int
    crystals: int
    recipe: str


class TypeRawStatusItem(TypedDict):
    """
    A dictionary storing information about the status of the raw data for a single run.

    Attributes:

        run_id: Unique identifier of the corresponding run.

        status: Status of the raw data from the run.
    """

    run_id: str
    status: str


class TypeTableRow(TypedDict):
    """
    A dictionary storing information from one row in the Cheetah GUI run table. The
    attributes of this dictionary correspond to the column names of the table.

    Attributes:

        Run: Unique identifier of a run in the Cheetah GUI run table.

        Dataset: The dataset tag assigned to the run.

        Rawdata: Status of the raw data from the run.

        Cheetah: Status of the data processing.

        H5Directory: Either the name of the processed directory (if processing has
            started) or "---".

        Nprocessed: Either the number of processed images (if processing has started)
            or "---".

        Nhits: Either the number of found hits (if processing has started) or "---".

        Nindexed:  Either the number of indexed hits (if streaming processing has
            started) or "---".

        Hitrate: Either hit rate in % (if processing has started) or "---".

        Idxrate: Either indexing rate in % or "---".

        Recipe: The name of the config file used for the data processing.

        Calibration: The name of the calibration file used for the data processing.
    """

    Run: str
    Rawdata: str
    Dataset: str
    Cheetah: str
    H5Directory: str
    Nprocessed: Union[int, Literal["---"]]
    Nhits: Union[int, Literal["---"]]
    Nindexed: Union[int, Literal["---"]]
    Hitrate: Union[float, Literal["---"]]
    Idxrate: Union[float, Literal["---"]]
    Recipe: str


class Crawler(ABC):
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
        output_filename: pathlib.Path,
        raw_directory_scan_enabled: bool = True,
        proc_directory_scan_enabled: bool = True,
    ) -> None:
        """
        Base class for Cheetah Crawler.

        Cheetah Crawlers implement methods which scan raw and processed data
        directories to fill the run table in the Cheetah GUI. They also provide methods
        to convert between run IDs displayed in the table and the names of the raw and
        processed data files and directories.

        This class is the base class from which every Crawler class should inherit.
        Each derived class should provide it's specific implementation of the abstract
        methods
        [_scan_raw_directory][cheetah.crawlers.base.Crawler._scan_raw_directory],
        [raw_id_to_table_id][cheetah.crawlers.base.Crawler.raw_id_to_table_id] and
        [table_id_to_raw_id][cheetah.crawlers.base.Crawler.table_id_to_raw_id].

        Arguments:

            raw_directory: The path of the raw data directory.

            proc_directory: The path of the processed data directory.

            output_filename: The path to the CSV file where Cheetah run table data will
                be written (usually cheetah/gui/crawler.txt).

        """
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._output_filename: pathlib.Path = output_filename
        self._raw_directory_scan_enabled: bool = raw_directory_scan_enabled
        self._proc_directory_scan_enabled: bool = proc_directory_scan_enabled

    @abstractmethod
    def _scan_raw_directory(self) -> List[TypeRawStatusItem]:
        # This function is called every time crawler updates the the run table
        # displayed in Cheetah GUI. It scans raw data directory and returns the list of
        # TypeRawStatusItem dictionaries containing each run ID and the status of the
        # raw data.
        pass

    @abstractmethod
    def raw_id_to_table_id(self, raw_id: str) -> str:
        """
        Convert raw run ID to table ID.

        This function converts unique identifier of the run derived from the raw data
        to the run ID displayed in the Cheetah GUI run table.

        Arguments:

            raw_id: Run ID of the raw data.

        Returns:

            Run ID displayed in the Cheetah GUI table.
        """
        pass

    @abstractmethod
    def table_id_to_raw_id(self, table_id: str) -> str:
        """
        Convert table run ID to raw ID.

        This function converts unique identifier of the run displayed in the Cheetah
        GUI run table to the raw data run ID.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        pass

    def raw_id_to_proc_id(self, raw_id: str) -> str:
        """
        Convert raw run ID to processed run ID.

        This function converts unique identifier of the run derived from the raw data
        to the run ID of the processed data. It raplaces all "-" signs in the raw ID
        by "_".

        Arguments:

            raw_id: Run ID of the raw data.

        Returns:

            Run ID of the processed data.
        """
        return raw_id.replace("-", "_")

    def _scan_proc_directory(self) -> List[TypeProcStatusItem]:
        # This function is called every time crawler updates the run table displayed
        # in Cheetah GUI. It scans processed data directory and returns the list of
        # TypeProcStatusItem dictionaries containing information of the data processing
        # status for each processing run.
        proc_status: List[TypeProcStatusItem] = []
        status_file: pathlib.Path
        for status_file in self._proc_directory.rglob("status.txt"):
            run_directory: pathlib.Path = status_file.parent
            run_name: str = str(run_directory.relative_to(self._proc_directory))
            split_items: List[str] = run_name.split("-")
            run_id: str = split_items[0]
            tag: str = "-".join(split_items[1:])
            recipe: str = ""
            process_config_file: pathlib.Path = run_directory / "process.config"
            if process_config_file.is_file():
                fh: TextIO
                with open(process_config_file, "r") as fh:
                    process_config: Dict[str, Any] = yaml.safe_load(fh.read())
                recipe = pathlib.Path(
                    process_config["Processing config"]["config_template"]
                ).name

            if status_file.is_file():
                with open(status_file, "r") as fh:
                    status: Dict[str, Any] = yaml.safe_load(fh.read())
                if "Update time" in status.keys():
                    update_time: float = datetime.strptime(
                        status["Update time"], "%a %b %d %H:%M:%S %Y"
                    ).timestamp()
                else:
                    update_time = status_file.stat().st_mtime
                if "Frames processed" in status.keys():
                    processed: int = status["Frames processed"]
                else:
                    processed = 0
                if "Number of hits" in status.keys():
                    hits: int = status["Number of hits"]
                else:
                    hits = 0
                crystfel_output: pathlib.Path = run_directory / "crystfel.out"
                if crystfel_output.is_file():
                    indexing_results: Tuple[int, int] = self._scan_crystfel_output(
                        crystfel_output
                    )
                else:
                    indexing_results = (-1, -1)
                proc_status.append(
                    {
                        "run_name": run_name,
                        "run_id": run_id,
                        "tag": tag,
                        "status": status["Status"],
                        "update_time": update_time,
                        "processed": processed,
                        "hits": hits,
                        "indexed": indexing_results[0],
                        "crystals": indexing_results[1],
                        "recipe": recipe,
                    }
                )

        return proc_status

    def _scan_crystfel_output(self, crystfel_output: pathlib.Path) -> Tuple[int, int]:
        fh: TextIO
        indexed: int = 0
        crystals: int = 0
        with open(crystfel_output) as fh:
            for line in fh.readlines()[::-1]:
                if line.endswith("images/sec.\n"):
                    split_items: List[str] = line.split()
                    indexed = int(split_items[6])
                    crystals = int(split_items[-4])
                    break
        return indexed, crystals

    def _read_table(self) -> Tuple[List[TypeRawStatusItem], List[TypeProcStatusItem]]:
        # Reads data from the crawler CSV file.
        raw_status: List[TypeRawStatusItem] = []
        proc_status: List[TypeProcStatusItem] = []
        if self._output_filename.exists():
            csvfile: TextIO
            with open(self._output_filename, "r") as csvfile:
                reader: csv.DictReader[str] = csv.DictReader(csvfile)
                table_row: Dict[str, str]
                for table_row in reader:
                    if table_row["Run"]:
                        raw_status.append(
                            {
                                "run_id": self.table_id_to_raw_id(table_row["Run"]),
                                "status": table_row["Rawdata"],
                            }
                        )
                    if table_row["H5Directory"] != "---":
                        split_items: List[str] = table_row["H5Directory"].split("-")
                        run_id: str = split_items[0]
                        tag: str = "-".join(split_items[1:])
                        update_time: float = -time.time()
                        if "Nindexed" in table_row and table_row["Nindexed"] != "---":
                            indexed: int = int(table_row["Nindexed"])
                        else:
                            indexed = -1
                        proc_status.append(
                            {
                                "run_name": table_row["H5Directory"],
                                "run_id": run_id,
                                "tag": tag,
                                "status": table_row["Cheetah"],
                                "update_time": update_time,
                                "processed": int(table_row["Nprocessed"]),
                                "hits": int(table_row["Nhits"]),
                                "indexed": indexed,
                                "crystals": -1,
                                "recipe": table_row["Recipe"],
                            }
                        )
        return raw_status, proc_status

    def _write_table(self, table_rows: List[TypeTableRow]) -> None:
        # Writes a list of TypeTableRow dictionaries to the output CSV file using the
        # keys of TypeTableRow as column names.
        csvfile: TextIO
        with open(self._output_filename, "w", newline="") as csvfile:
            writer: csv.DictWriter[str] = csv.DictWriter(
                csvfile, fieldnames=list(TypeTableRow.__annotations__.keys())
            )
            writer.writeheader()
            row: TypeTableRow
            for row in table_rows:
                writer.writerow(row)

    def raw_directory_scan_is_enabled(self) -> bool:
        """
        Checks if raw directory scan is enabled.

        When raw directory scan is enabled it is performed every time
        [`update`][cheetah.crawlers.base.Crawler.update] function is called. When raw
        directory scan is disabled the status of the raw data is retrieved from the
        crawler CSV file instead.

        Returns:

            True if raw directory scan is enabled, False if disabled.
        """
        return self._raw_directory_scan_enabled

    def proc_directory_scan_is_enabled(self) -> bool:
        """
        Checks if processed directory scan is enabled.

        When processed directory scan is enabled it is performed every time
        [`update`][cheetah.crawlers.base.Crawler.update] function is called. When
        processed directory scan is disabled the status of the raw data is retrieved
        from the crawler CSV file instead.

        Returns:

            True if processed directory scan is enabled, False if disabled.
        """
        return self._proc_directory_scan_enabled

    def set_raw_directory_scan_enabled(self, enable: bool = True) -> None:
        """
        Enables or disables raw directory scan.

        When raw directory scan is enabled it is performed every time
        [`update`][cheetah.crawlers.base.Crawler.update] function is called. When raw
        directory scan is disabled the status of the raw data is retrieved from the
        crawler CSV file instead.

        Arguments:

            enable: Whether to enable raw directory scanning. Defaults to True.
        """
        self._raw_directory_scan_enabled = enable

    def set_proc_directory_scan_enabled(self, enable: bool = True) -> None:
        """
        Enables or disables processed directory scan.

        When processed directory scan is enabled it is performed every time
        [`update`][cheetah.crawlers.base.Crawler.update] function is called. When
        processed directory scan is disabled the status of the raw data is retrieved
        from the crawler CSV file instead.

        Arguments:

            enable: Whether to enable raw directory scanning. Defaults to True.
        """
        self._proc_directory_scan_enabled = enable

    def update(self) -> None:
        """
        Updates CSV file containing information for the Cheetah GUI run table.

        This function is called periodically to update the run table displayed by
        Cheetah GUI. It scans raw and processed data directories and fills the list
        of [TypeTableRow][cheetah.crawlers.base.TypeTableRow] dictionaries with the
        corresponding data for each run. If there is more than one processed directory
        corresponding to a particular run, it takes the one which was updated last. It
        then writes the accumulated data to the output CSV file using the keys of
        [TypeTableRow][cheetah.crawlers.base.TypeTableRow] dictionary as column names.
        """
        if (
            self._raw_directory_scan_enabled is False
            and self._proc_directory_scan_enabled is False
        ):
            print(
                "Crawler: both raw and hdf5 directory scanning is disabled, "
                "doing nothing."
            )
        raw_status: List[TypeRawStatusItem]
        proc_status: List[TypeProcStatusItem]
        raw_status, proc_status = self._read_table()
        if self._raw_directory_scan_enabled:
            print("Crawler: scanning raw directory")
            raw_status = self._scan_raw_directory()
        if self._proc_directory_scan_enabled:
            print("Crawler: scanning hdf5 directory")
            proc_status = self._scan_proc_directory()
        proc_status = sorted(proc_status, key=itemgetter("update_time"), reverse=True)

        raw_status_item: TypeRawStatusItem
        table_rows: List[TypeTableRow] = []
        for raw_status_item in raw_status:
            raw_id: str = raw_status_item["run_id"]
            proc_id: str = self.raw_id_to_proc_id(raw_id)
            proc_status_item: TypeProcStatusItem

            row: TypeTableRow = cast(
                TypeTableRow,
                {key: "---" for key in TypeTableRow.__annotations__.keys()},
            )
            row["Run"] = self.raw_id_to_table_id(raw_id)
            row["Rawdata"] = raw_status_item["status"]
            table_rows.append(row)

            n_proc_items_run = 0
            for proc_status_item in proc_status:
                if proc_status_item["run_id"] == proc_id:
                    if n_proc_items_run > 0:
                        row = cast(
                            TypeTableRow,
                            {key: "---" for key in TypeTableRow.__annotations__.keys()},
                        )
                        row["Run"] = ""
                        row["Rawdata"] = ""
                        table_rows.append(row)
                    row["Dataset"] = proc_status_item["tag"]
                    row["H5Directory"] = proc_status_item["run_name"]
                    row["Cheetah"] = proc_status_item["status"]
                    row["Recipe"] = proc_status_item["recipe"]

                    hits: int = proc_status_item["hits"]
                    processed: int = proc_status_item["processed"]
                    hitrate: Union[Literal["---"], float] = (
                        100 * hits / processed if processed > 0 else "---"
                    )
                    indexed: int = proc_status_item["indexed"]
                    if indexed >= 0:
                        row["Nindexed"] = indexed
                        row["Idxrate"] = 100 * indexed / hits if hits > 0 else "---"
                    row["Nprocessed"] = processed
                    row["Nhits"] = hits
                    row["Hitrate"] = hitrate
                    n_proc_items_run += 1

        self._write_table(table_rows)
    
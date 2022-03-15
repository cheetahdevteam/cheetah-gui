"""
Cheetah Crawler's base classes.

This module contains base classes for Cheetah Crawlers.
"""
import csv
import pathlib

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, TextIO, Dict, Union, cast

try:
    from typing import TypedDict, Literal
except:
    from typing_extensions import TypedDict, Literal  # type: ignore


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

        recipe: The name of the config file used for the data processing.
    """

    run_id: str
    run_name: str
    tag: str
    status: str
    update_time: float
    processed: int
    hits: int
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

        Hitrate: Either hit rate in % (if processing has started) or "---".

        Recipe: The name of the config file used for the data processing.

        Calibration: The name of the calibration file used for the data processing.
    """

    Run: str
    Dataset: str
    Rawdata: str
    Cheetah: str
    H5Directory: str
    Nprocessed: Union[int, Literal["---"]]
    Nhits: Union[int, Literal["---"]]
    Hitrate: Union[float, Literal["---"]]
    Recipe: str
    Calibration: str


class Crawler(ABC):
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
        output_filename: pathlib.Path,
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
        # This function is called every time crawler updates the the run table
        # displayed in Cheetah GUI. It scans processed data directory and returns the
        # list of TypeProcStatusItem dictionaries containing information of the data
        # processing status for each processing run.
        hdf5_status: List[TypeProcStatusItem] = []
        status_file: pathlib.Path
        for status_file in self._proc_directory.rglob("status.txt"):
            run_directory: pathlib.Path = status_file.parent
            run_name: str = str(run_directory.relative_to(self._proc_directory))
            split_items: List[str] = run_name.split("-")
            run_id: str = split_items[0]
            tag: str = "-".join(split_items[1:])
            recipe: str = ""
            process_config_file: pathlib.Path = run_directory / "process_config.txt"
            if process_config_file.is_file():
                process_config: Dict[str, str] = self._parse_status_file(
                    process_config_file
                )
                if "config_template" in process_config.keys():
                    recipe = pathlib.Path(process_config["config_template"]).name

            if status_file.is_file():
                status: Dict[str, str] = self._parse_status_file(status_file)
                if "Update time" in status.keys():
                    update_time: float = datetime.strptime(
                        status["Update time"], "%a %b %d %H:%M:%S %Y"
                    ).timestamp()
                else:
                    update_time = status_file.stat().st_mtime
                if "Frames processed" in status.keys():
                    processed: int = int(status["Frames processed"])
                else:
                    processed = 0
                if "Number of hits" in status.keys():
                    hits: int = int(status["Number of hits"])
                else:
                    hits = 0
                hdf5_status.append(
                    {
                        "run_name": run_name,
                        "run_id": run_id,
                        "tag": tag,
                        "status": status["Status"],
                        "update_time": update_time,
                        "processed": processed,
                        "hits": hits,
                        "recipe": recipe,
                    }
                )

        return hdf5_status

    def _parse_status_file(self, filename: pathlib.Path) -> Dict[str, str]:
        # This function parses status.txt file written by Cheetah processing.
        status: Dict[str, str] = {}
        fh: TextIO
        with open(filename, "r") as fh:
            line: str
            for line in fh:
                line_items: List[str] = line.split(":")
                if len(line_items) > 1:
                    status[line_items[0].strip()] = ":".join(line_items[1:]).strip()
        return status

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
        print("Crawler: scanning raw directory")
        raw_status: List[TypeRawStatusItem] = self._scan_raw_directory()
        print("Crawler: scanning hdf5 directory")
        proc_status: List[TypeProcStatusItem] = self._scan_proc_directory()
        raw_status_item: TypeRawStatusItem
        table_rows: List[TypeTableRow] = []
        for raw_status_item in raw_status:
            raw_id: str = raw_status_item["run_id"]
            proc_id: str = self.raw_id_to_proc_id(raw_id)
            latest_update_time: float = -1
            latest_proc_item: Union[None, TypeProcStatusItem] = None
            proc_status_item: TypeProcStatusItem
            for proc_status_item in proc_status:
                if (
                    proc_status_item["run_id"] == proc_id
                    and latest_update_time < proc_status_item["update_time"]
                ):
                    latest_proc_item = proc_status_item
                    latest_update_time = latest_proc_item["update_time"]
            row: TypeTableRow = cast(
                TypeTableRow,
                {key: "---" for key in TypeTableRow.__annotations__.keys()},
            )
            row["Run"] = self.raw_id_to_table_id(raw_id)
            row["Rawdata"] = raw_status_item["status"]
            if latest_proc_item:
                row["Dataset"] = latest_proc_item["tag"]
                row["H5Directory"] = latest_proc_item["run_name"]
                row["Cheetah"] = latest_proc_item["status"]
                row["Recipe"] = latest_proc_item["recipe"]

                hits: int = latest_proc_item["hits"]
                processed: int = latest_proc_item["processed"]
                hitrate: Union[Literal["---"], float] = (
                    100 * hits / processed if processed > 0 else "---"
                )
                row["Nprocessed"] = processed
                row["Nhits"] = hits
                row["Hitrate"] = hitrate

            table_rows.append(row)
        self._write_table(table_rows)

    def _write_table(self, table_rows: List[TypeTableRow]) -> None:
        # Writes a list of TypeTableRow dictionaries to the output CSV file using the
        # keys of TypeTableRow as column names.
        with open(self._output_filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=list(TypeTableRow.__annotations__.keys())
            )
            writer.writeheader()
            for row in table_rows:
                writer.writerow(row)

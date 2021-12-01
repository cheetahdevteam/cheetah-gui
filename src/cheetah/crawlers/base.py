import csv
import pathlib

from abc import ABC, abstractmethod
from datetime import datetime
from typing import List, TextIO, Dict, TypedDict, Union, cast


class TypeProcStatusItem(TypedDict):
    run_name: str
    run_id: str
    tag: str
    status: str
    update_time: float
    processed: int
    hits: int


class TypeRawStatusItem(TypedDict):
    run_id: str
    status: str


TypeTableRow = TypedDict(
    "TypeTableRow",
    {
        "Run": str,
        "Dataset": str,
        "Rawdata": str,
        "Cheetah": str,
        "CrystFEL": str,
        "H5Directory": str,
        "Nprocessed": Union[int, str],
        "Nhits": Union[int, str],
        "Nindex": Union[int, str],
        "Hitrate%": Union[float, str],
        "Recipe": str,
        "Calibration": str,
    },
)


class Crawler(ABC):
    """
    See documentation of the `__init__` function.

    Base class: `ABC`
    """

    def __init__(
        self,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
        indexing_directory: pathlib.Path,
        output_filename: pathlib.Path,
    ) -> None:
        """ """
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._indexing_directory: pathlib.Path = indexing_directory
        self._output_filename: pathlib.Path = output_filename

    @abstractmethod
    def _scan_raw_directory(self) -> List[TypeRawStatusItem]:
        pass

    @abstractmethod
    def raw_id_to_table_id(self, raw_id: str) -> str:
        """ """
        pass

    @abstractmethod
    def table_id_to_raw_id(self, table_id: str) -> str:
        """ """
        pass

    def raw_id_to_proc_id(self, raw_id: str) -> str:
        """ """
        return raw_id.replace("-", "_")

    def _scan_proc_directory(self) -> List[TypeProcStatusItem]:
        hdf5_status: List[TypeProcStatusItem] = []
        run_directory: pathlib.Path
        for run_directory in self._proc_directory.glob("*"):
            status_file: pathlib.Path = run_directory / "status.txt"
            run_name: str = run_directory.name
            split_items: List[str] = run_name.split("-")
            run_id: str = split_items[0]
            tag: str = "-".join(split_items[1:])
            if status_file.is_file():
                status: Dict[str, str] = self._parse_status_file(status_file)
                if "Update time" in status.keys():
                    update_time: float = datetime.strptime(
                        status["Update time"], "%a %b %d %H:%M:%S %Y"
                    ).timestamp()
                else:
                    update_time = 0
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
                    }
                )
        return hdf5_status

    def _parse_status_file(self, filename: pathlib.Path) -> Dict[str, str]:
        status: Dict[str, str] = {}
        fh: TextIO
        with open(filename, "r") as fh:
            line: str
            for line in fh:
                line_items: List[str] = line.split(":")
                if len(line_items) > 1:
                    status[line_items[0].strip()] = ":".join(line_items[1:]).strip()
        return status

    def _scan_indexing_directory(self) -> None:
        pass

    def update(self) -> None:
        print("Crawler: scanning raw directory")
        raw_status: List[TypeRawStatusItem] = self._scan_raw_directory()
        print("Crawler: scanning hdf5 directory")
        proc_status: List[TypeProcStatusItem] = self._scan_proc_directory()
        raw_status_item: TypeRawStatusItem
        table_rows: List[TypeTableRow] = []
        for raw_status_item in sorted(raw_status, key=lambda i: i["run_id"]):
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

                hits: int = latest_proc_item["hits"]
                processed: int = latest_proc_item["processed"]
                hitrate: Union[str, float] = (
                    100 * hits / processed if processed > 0 else "---"
                )
                row["Nprocessed"] = processed
                row["Nhits"] = hits
                row["Hitrate%"] = hitrate

            table_rows.append(row)
        self._write_table(table_rows)

    def _write_table(self, table_rows: List[TypeTableRow]) -> None:
        with open(self._output_filename, "w", newline="") as csvfile:
            writer = csv.DictWriter(
                csvfile, fieldnames=list(TypeTableRow.__annotations__.keys())
            )
            writer.writeheader()
            for row in table_rows:
                writer.writerow(row)

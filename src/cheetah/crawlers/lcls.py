import pathlib

from cheetah.crawlers.base import Crawler, TypeRawStatusItem
from typing import Any, List, TextIO, Dict


class LclsCrawler(Crawler):
    """
    See documentation of the `__init__` function.

    Base class: [`Crawler`][cheetah.crawlers.base]
    """

    def _scan_raw_directory(self) -> List[TypeRawStatusItem]:
        """ """
        status: Dict[str, str] = {}
        filename: pathlib.Path
        for filename in self._raw_directory.glob("*.xtc*"):
            run_id: str = filename.name.split("-")[1]
            if run_id not in status.keys():
                status[run_id] = "Ready"
            if filename.suffix == ".inprogress":
                status[run_id] = "Copying"
            elif filename.suffix == ".fromtape":
                status[run_id] = "Restoring"

        raw_status: List[TypeRawStatusItem] = []
        for run_id in status:
            raw_status.append({"run_id": run_id, "status": status[run_id]})
        return raw_status

    def raw_id_to_table_id(self, raw_id: str) -> str:
        """ """
        return str(int(raw_id[1:]))

    def table_id_to_raw_id(self, table_id: str) -> str:
        """ """
        return f"r{int(table_id):04d}"

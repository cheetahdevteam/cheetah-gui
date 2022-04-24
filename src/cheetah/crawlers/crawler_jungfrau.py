"""
Jungfrau 1M Files Crawler.

This module contains Cheetah Crawler for Jungfray 1M files.
"""
import h5py  # type: ignore
import pathlib
import re
import time

from datetime import datetime
from cheetah.crawlers.base import Crawler, TypeRawStatusItem
from typing import List, Set, Any


class Jungfrau1MCrawler(Crawler):
    """
    Cheetah Crawler for Jungfrau 1M detector.
    """

    def _get_timestamp_from_master_file(self, filename: pathlib.Path) -> float:
        # Gets timestamp from Jungfrau 1M master h5 file.
        h5file: Any
        with h5py.File(filename, "r") as h5file:
            timestamp: float = datetime.strptime(
                h5file["/entry/instrument/detector/timestamp"][()]
                .decode("utf-8")
                .strip(),
                "%a %b %d %H:%M:%S %Y",
            ).timestamp()

        return timestamp

    def _scan_raw_directory(self) -> List[TypeRawStatusItem]:
        # This function scans raw data directory and returns the list of
        # TypeRawStatusItem dictionaries containing ID and the status of the raw data
        # for each run. It finds all
        # {relative_run_directory_path}/{run_name}_master_*.h5 files in the raw data
        # directory and uses '{relative_run_directory_path}/{run_name}' string as the
        # raw run ID. If the latest master_*.h5 file was last modified more than a
        # minite ago it sets the status of the raw data to 'Ready'. Otherwise, it sets
        # the status to 'In progress'.

        run_id_pattern: re.Pattern[str] = re.compile(r"(.+)_master_\d+\.h5")
        raw_status: List[TypeRawStatusItem] = []
        run_ids: Set[str] = set()
        filename: pathlib.Path
        current_time: float = time.time()
        for filename in sorted(
            self._raw_directory.glob("**/*_master_*.h5"),
            key=self._get_timestamp_from_master_file,
        ):
            run_id: str = run_id_pattern.findall(
                str(filename.relative_to(self._raw_directory))
            )[0]
            mtime: float = filename.stat().st_mtime
            if current_time - mtime > 60:
                status: str = "Ready"
            else:
                status = "In progress"

            if run_id not in run_ids:
                raw_status.append({"run_id": run_id, "status": status})
                run_ids.add(run_id)
            else:
                raw_status[-1]["status"] = status

        return raw_status

    def raw_id_to_table_id(self, raw_id: str) -> str:
        """
        Convert raw run ID to table ID.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        Junfrau 1M detector writes data to HDF5 files with the following path:
        {raw_path}/{relative_run_directory_path}/{run_name}_d*.h5.
        Cheetah uses '{relative_run_directory_path}/{run_name}' string as both the raw
        run ID and the run ID displayed in the Chetah GUI table.

        Arguments:

            raw_id: Run ID of the raw data.

        Returns:

            Run ID displayed in the Cheetah GUI table.
        """

        return raw_id

    def table_id_to_raw_id(self, table_id: str) -> str:
        """
        Convert table run ID to raw ID.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        Junfrau 1M detector writes data to HDF5 files with the following path:
        {raw_path}/{relative_run_directory_path}/{run_name}_d*.h5.
        Cheetah uses '{relative_run_directory_path}/{run_name}' string as both the raw
        run ID and the run ID displayed in the Chetah GUI table.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        return table_id

"""
P11 Petra III Crawler.

This module contains Cheetah Crawler for P11 beamline at PETRA III.
"""

import pathlib
import time
from typing import List

from cheetah.crawlers.base import Crawler, RawStatusItem


class P11EigerCrawler(Crawler):
    """
    Cheetah Crawler for Eiger 16M detector at P11 beamline at PETRA III.
    """

    def _scan_raw_directory(self) -> List[RawStatusItem]:
        # This function scans raw data directory and returns the list of
        # TypeRawStatusItem dictionaries containing ID and the status of the raw data
        # for each run. It finds all {relative_run_directory_path}/{run_name}_master.h5
        # files in the raw data directory and uses
        # '{relative_run_directory_path}/{run_name}' string as the raw run ID. If the
        # master.h5 file was last modified more than a minite ago it sets the status of
        # the raw data to 'Ready'. Otherwise, it sets the status to 'In progress'.

        raw_status: List[RawStatusItem] = []
        filename: pathlib.Path
        current_time: float = time.time()
        for filename in sorted(
            self._raw_directory.rglob("*_master.h5"), key=lambda f: f.stat().st_mtime
        ):
            run_id: str = str(filename.relative_to(self._raw_directory))[:-10]
            mtime: float = filename.stat().st_mtime
            if current_time - mtime > 60:
                status: str = "Ready"
            else:
                status = "In progress"
            raw_status.append({"run_id": run_id, "status": status})

        return raw_status

    def raw_id_to_table_id(self, raw_id: str) -> str:
        """
        Convert raw run ID to table ID.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        Eiger detector writes data to HDF5 files with the following path:
        {raw_path}/{relative_run_directory_path}/{run_name}_data_{file_number}.h5.
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

        Eiger detector writes data to HDF5 files with the following path:
        {raw_path}/{relative_run_directory_path}/{run_name}_data_{file_number}.h5.
        Cheetah uses '{relative_run_directory_path}/{run_name}' string as both the raw
        run ID and the run ID displayed in the Chetah GUI table.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        return table_id

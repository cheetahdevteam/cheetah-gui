"""
LCLS Crawler.

This module contains Cheetah Crawler for LCLS.
"""

import pathlib
from typing import Dict, List

from cheetah.crawlers.base import Crawler, RawStatusItem


class LclsCrawler(Crawler):
    """
    Cheetah Crawler for LCLS.
    """

    def _scan_raw_directory(self) -> List[RawStatusItem]:
        # This function scans raw data directory and returns the list of
        # TypeRawStatusItem dictionaries containing ID and the status of the raw data
        # for each run. At LCLS the name of the raw data file starts with "r{NNNN}-"
        # where {NNNN} is the run number, therefore the part of the filename before the
        # first "-" is used as the raw run ID.
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

        raw_status: List[RawStatusItem] = []
        for run_id in sorted(status):
            raw_status.append(RawStatusItem(run_id=run_id, status=status[run_id]))
        return raw_status

    def raw_id_to_table_id(self, raw_id: str) -> str:
        """
        Convert raw run ID to table ID.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        At LCLS the raw run ID has the form of "r{NNNN}" where {NNNN} is the run
        number. The run ID displayed in the Cheetah GUI table is the run number without
        leading zeros.

        Arguments:

            raw_id: Run ID of the raw data.

        Returns:

            Run ID displayed in the Cheetah GUI table.
        """
        return str(int(raw_id[1:]))

    def table_id_to_raw_id(self, table_id: str) -> str:
        """
        Convert table run ID to raw ID.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        At LCLS the raw run ID has the form of "rNNNN" where NNNN is the run number.
        The run ID displayed in the Cheetah GUI table is the run number without leading
        zeros.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        return f"r{int(table_id):04d}"

"""
BioCARS APS Crawler.

This module contains Cheetah Crawler for BioCARS 14-ID-B beamline at APS.
"""
import pathlib
import subprocess
import time

from cheetah.crawlers.base import Crawler, TypeRawStatusItem
from typing import List


class BioCarsMccdCrawler(Crawler):
    """
    Cheetah Crawler for Rayonix MX340-HS detector at BioCARS beamline at APS.
    """

    def _scan_raw_directory(self) -> List[TypeRawStatusItem]:
        # This function scans raw data directory and returns the list of
        # TypeRawStatusItem dictionaries containing ID and the status of the raw data
        # for each run. It finds all sub-directories which contain .mccd files in the
        # raw data directory and uses relative directory path as the raw run ID. If the
        # latest .mccd file in the run was last modified more than a minite ago it sets
        # the status of the raw data to 'Ready'. Otherwise, it sets the status to
        # 'In progress'.

        raw_status: List[TypeRawStatusItem] = []
        child_directory: pathlib.Path
        for child_directory in self._raw_directory.glob("**/"):
            # Check if there're mccd files in the child directory
            if next(child_directory.glob("*.mccd"), None) is None:
                continue
            run_id: str = str(child_directory.relative_to(self._raw_directory))
            command = (
                f'find {child_directory} -maxdepth 1 -name "*.mccd" -printf '
                f'"%C@\\n" | sort | tail -n 1'
            )
            latest_mtime: float = float(
                subprocess.run(command, shell=True, capture_output=True).stdout
            )
            current_time: float = time.time()
            if current_time - latest_mtime > 60:
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

        For the data collected at BioCARS beamline at APS using Rayonix detecor
        Cheetah uses run directory path relative to the raw experiment directory as
        both the raw run ID and the run ID displayed in the Chetah GUI table.

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

        For the data collected at BioCARS beamline at APS using Rayonix detecor
        Cheetah uses run directory path relative to the raw experiment directory as
        both the raw run ID and the run ID displayed in the Chetah GUI table.

        Arguments:

            table_id: Run ID displayed in the Cheetah GUI table.

        Returns:

            Run ID of the raw data.
        """
        return table_id

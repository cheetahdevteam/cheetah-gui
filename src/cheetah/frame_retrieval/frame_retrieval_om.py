"""
Frame retrieval from OM data retrieval.
"""
import logging

from typing import Any, Dict, List, TextIO, cast

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.frame_retrieval.base import (
    CheetahFrameRetrieval,
    TypeEventData,
    TypePeakList,
)

from om.algorithms.crystallography import TypePeakList as OmTypePeakList
from om.data_retrieval_layer import OmEventDataRetrieval
from om.lib.crystallography import CrystallographyPeakFinding
from om.lib.geometry import GeometryInformation
from om.lib.parameters import MonitorParameters


class _TypeOmEvent(TypedDict):
    # A dictionary used internally to store information about a single data event which
    # can be retrieved using OM frame retrieval.

    filename: str
    event_id: str


class OmRetrieval(CheetahFrameRetrieval):
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, sources: List[str], parameters: Dict[str, Any]):
        """
        Frame Retrieval from OM data retrieval layer.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This class implements Cheetah Frame Retrieval from OM data retrieval layer. The
        sources required by this Frame Retrieval class are the names of the text files
        each containing a list of OM event IDs. Each of the source files requires two
        parameters: OM source string and OM config file. Additionaly, one may provide
        Cheetah peak list files containing information about detected peaks for the
        events in each source file. If the peak list file is not provided for any of
        the source files
        [get_data][cheetah.frame_retrieval.frame_retrieval_om.OmRetrieval.get_data]
        function will perform peak detection for the events in this file using
        peakfinder8 parameters from the corresponding OM config file.

        Arguments:

            sources: A list of text files each containing a list of OM event IDs.

            parameters: A dictionary containing configuration parameters for data
                retrieval from OM data retrieval layer.

                The following parameteres are required:

                * `om_sources`: A dictionary where the keys are each of the provided
                  `sources` and the values are corresponding to them OM source strings.

                * `om_configs`: A dictionary where the keys are each of the provided
                  `sources` and the values are corresponding to them OM config files.

                The following parameters are optional:

                * `peak_lists`: A dictionary where each key is one of the provided
                  `sources` and the values are corresponding to then Cheetah peak list
                  files.
        """
        self._om_retrievals: Dict[str, OmEventDataRetrieval] = {}
        self._events: List[_TypeOmEvent] = []
        self._peak_lists: Dict[str, Dict[str, TypePeakList]] = {}
        self._peakfinders: Dict[str, CrystallographyPeakFinding] = {}
        filename: str
        for filename in sources:
            fh: TextIO
            if filename not in parameters["om_sources"].keys():
                logging.warning(
                    f"OM source string for event list file {filename} is not"
                    f"provided. Events from this file won't be retrieved."
                )
                continue

            if filename not in parameters["om_configs"].keys():
                logging.warning(
                    f"OM config file for event list file {filename} is not"
                    f"provided. Events from this file won't be retrieved."
                )
                continue

            with open(filename, "r") as fh:
                line: str
                event_ids: List[str] = [line.strip() for line in fh]
                if len(event_ids) > 0:
                    try:
                        monitor_params: MonitorParameters = MonitorParameters(
                            config=parameters["om_configs"][filename]
                        )
                        self._om_retrievals[filename] = OmEventDataRetrieval(
                            source=parameters["om_sources"][filename],
                            monitor_parameters=monitor_params,
                        )
                    except Exception as e:
                        logging.exception(
                            f"Couldn't initialize OM frame retrieval from "
                            f"{parameters['om_sources'][filename]} source using "
                            f"{parameters['om_configs'][filename]} config file: "
                        )
                        continue
                    self._events.extend(
                        [{"filename": filename, "event_id": eid} for eid in event_ids]
                    )
                    if (
                        "peak_lists" in parameters.keys()
                        and filename in parameters["peak_lists"].keys()
                    ):
                        if monitor_params.get_parameter(
                            group="crystallography",
                            parameter="binning",
                            parameter_type=bool,
                        ):
                            bin_size: int = monitor_params.get_parameter(
                                group="binning",
                                parameter="bin_size",
                                parameter_type=int,
                                required=True,
                            )
                        else:
                            bin_size = 1
                        self._peak_lists[filename] = self._load_peaks_from_file(
                            parameters["peak_lists"][filename], bin_size
                        )
                    else:
                        geometry_information: GeometryInformation = GeometryInformation(
                            geometry_filename=monitor_params.get_parameter(
                                group="crystallography",
                                parameter="geometry_file",
                                parameter_type=str,
                                required=True,
                            )
                        )
                        self._peakfinders[filename] = CrystallographyPeakFinding(
                            parameters=monitor_params,
                            geometry_information=geometry_information,
                        )

        self._num_events: int = len(self._events)

    def _load_peaks_from_file(
        self, filename: str, bin_size: int = 1
    ) -> Dict[str, TypePeakList]:
        # Loads peaks from the peak list file written by Cheetah processing.
        # If binning was used (bin_size > 1) transforms peak positions to match the
        # original image size.
        peaks: Dict[str, TypePeakList] = {}
        previous_id: str = ""
        fh: TextIO
        with open(filename) as fh:
            fh.readline()
            line: str
            for line in fh:
                split_items: List[str] = line.split(",")
                event_id = split_items[0].strip()
                if event_id != previous_id:
                    peaks[event_id] = {
                        "num_peaks": int(split_items[1]),
                        "fs": [],
                        "ss": [],
                    }
                    previous_id = event_id
                peaks[event_id]["fs"].append(
                    (float(split_items[2]) + 0.5) * bin_size - 0.5
                )
                peaks[event_id]["ss"].append(
                    (float(split_items[3]) + 0.5) * bin_size - 0.5
                )
        return peaks

    def get_event_list(self) -> List[str]:
        """
        Get the list of events from OM data retrieval layer.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function returns a list of all event IDs which can be retrieved from the
        list of the source files using OM frame retrieval.

        Returns:

            A list of event IDs.
        """
        event: _TypeOmEvent
        return [event["event_id"] for event in self._events]

    def get_data(self, event_index: int) -> TypeEventData:
        """
        Get all available frame data for a requested event.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function retrieves all available data related to the requested event
        using OM frame retrieval. This includes detector data, photon energy in eV and
        detector distance in meters. Additionaly, it can retrieve a list of detected
        peaks either from the peak list file or by running peafinder8 algorithm using
        parameters from the provided OM config file.

        Arguments:

            event_index: Index of the event in the list of events.

        Returns:

            A [TypeEventData][cheetah.frame_retrieval.base.TypeEventData] dictionary
            containing all available data related to the requested event.
        """
        event_data: TypeEventData = {}
        filename: str = self._events[event_index]["filename"]
        event_id: str = self._events[event_index]["event_id"]

        data: Dict[str, Any] = self._om_retrievals[filename].retrieve_event_data(
            event_id=event_id
        )

        event_data["data"] = data["detector_data"]
        event_data["photon_energy"] = data["beam_energy"]
        event_data["clen"] = data["detector_distance"]

        if filename in self._peak_lists.keys():
            event_data["peaks"] = self._peak_lists[filename][event_id]
        elif filename in self._peakfinders.keys():
            peak_list: OmTypePeakList = self._peakfinders[filename].find_peaks(
                detector_data=event_data["data"]
            )
            event_data["peaks"] = {
                "num_peaks": peak_list["num_peaks"],
                "fs": peak_list["fs"],
                "ss": peak_list["ss"],
            }

        return event_data

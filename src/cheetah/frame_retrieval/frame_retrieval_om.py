"""
Frame retrieval from OM data retrieval.
"""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, TextIO

from om.algorithms.crystallography import PeakList as OmPeakList
from om.data_retrieval_layer.event_retrieval import OmEventDataRetrieval
from om.lib.crystallography import CrystallographyPeakFinding
from om.lib.exceptions import OmConfigurationFileSyntaxError
from om.lib.files import load_configuration_parameters
from om.lib.geometry import GeometryInformation
from pydantic import BaseModel, Field, ValidationError, model_validator
from typing_extensions import Self

from cheetah.frame_retrieval.base import CheetahFrameRetrieval, EventData, PeakList

logger: logging.Logger = logging.getLogger(__name__)


class _CrytallograhyParametersBinning(BaseModel):
    binning: bool


class _BinningParameters(BaseModel):
    bin_size: int = Field(default=None)


class _MonitorParametersBinning(BaseModel):
    crystallography: _CrytallograhyParametersBinning
    binning: _BinningParameters

    @model_validator(mode="after")
    def check_bin_size(self) -> Self:
        if self.crystallography.binning:
            if self.binning.bin_size is None:
                raise ValueError(
                    "When the value of the cyrstallography/binning entry in OM's "
                    "configuration parameters is true, the bin_size must be provided "
                    "via the binning/bin_size entry"
                )
        else:
            self.binning.bin_size = 1
        return self


class _CrystallograhyParametersGeometry(BaseModel):
    geometry_file: str


class _MonitorParametersGeometry(BaseModel):
    crystallography: _CrystallograhyParametersGeometry


@dataclass
class _OmEvent:
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
        self._events: List[_OmEvent] = []
        self._peak_lists: Dict[str, Dict[str, PeakList]] = {}
        self._peakfinders: Dict[str, CrystallographyPeakFinding] = {}

        filename: str
        for filename in sources:
            fh: TextIO
            if filename not in parameters["om_sources"]:
                logger.warning(
                    f"OM source string for event list file {filename} is not"
                    f"provided. Events from this file won't be retrieved."
                )
                continue

            if filename not in parameters["om_configs"]:
                logger.warning(
                    f"OM config file for event list file {filename} is not"
                    f"provided. Events from this file won't be retrieved."
                )
                continue

            with open(filename, "r") as fh:
                event_ids: List[str] = [line.strip() for line in fh]
                if len(event_ids) > 0:
                    try:
                        monitor_parameters: Dict[str, Dict[str, Any]] = (
                            load_configuration_parameters(
                                config=Path(parameters["om_configs"][filename])
                            )
                        )
                        self._om_retrievals[filename] = OmEventDataRetrieval(
                            source=parameters["om_sources"][filename],
                            parameters=monitor_parameters,
                        )
                    except Exception:
                        logger.exception(
                            f"Couldn't initialize OM frame retrieval from "
                            f"{parameters['om_sources'][filename]} source using "
                            f"{parameters['om_configs'][filename]} config file: "
                        )
                        continue
                    self._events.extend(
                        [_OmEvent(filename=filename, event_id=eid) for eid in event_ids]
                    )
                    if (
                        "peak_lists" in parameters
                        and "filename" in parameters["peak_lists"]
                    ):
                        try:
                            binning_parameters: _MonitorParametersBinning = (
                                _MonitorParametersBinning.model_validate(
                                    monitor_parameters
                                )
                            )
                        except ValidationError as exception:
                            raise OmConfigurationFileSyntaxError(
                                "Error parsing OM's Configuration parameters: "
                                f"{exception}"
                            )

                        self._peak_lists[filename] = self._load_peaks_from_file(
                            parameters["peak_lists"][filename],
                            binning_parameters.binning.bin_size,
                        )
                    else:
                        try:
                            geometry_parameters: _MonitorParametersGeometry = (
                                _MonitorParametersGeometry.model_validate(
                                    monitor_parameters
                                )
                            )
                        except ValidationError as exception:
                            raise OmConfigurationFileSyntaxError(
                                "Error parsing OM's Configuration parameters: "
                                f"{exception}"
                            )

                        geometry_information: GeometryInformation = (
                            GeometryInformation.from_file(
                                geometry_filename=geometry_parameters.crystallography.geometry_file
                            )
                        )
                        self._peakfinders[filename] = CrystallographyPeakFinding(
                            parameters=monitor_parameters,
                            geometry_information=geometry_information,
                        )

        self._num_events: int = len(self._events)

    def _load_peaks_from_file(
        self, filename: str, bin_size: int = 1
    ) -> Dict[str, PeakList]:
        # Loads peaks from the peak list file written by Cheetah processing.
        # If binning was used (bin_size > 1) transforms peak positions to match the
        # original image size.
        peaks: Dict[str, PeakList] = {}
        previous_id: str = ""
        fh: TextIO
        with open(filename) as fh:
            fh.readline()
            line: str
            for line in fh:
                split_items: List[str] = line.split(",")
                event_id = split_items[0].strip()
                if event_id != previous_id:
                    try:
                        peaks[event_id] = PeakList(
                            num_peaks=int(split_items[1]),
                            fs=[],
                            ss=[],
                        )
                        previous_id = event_id
                    except Exception:
                        # TODO: figure out why it breaks here at random times
                        continue
                peaks[event_id].fs.append(
                    (float(split_items[2]) + 0.5) * bin_size - 0.5
                )
                peaks[event_id].ss.append(
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
        return [event.event_id for event in self._events]

    def get_data(self, event_index: int) -> EventData:
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
        event_data: EventData = EventData()
        filename: str = self._events[event_index].filename
        event_id: str = self._events[event_index].event_id

        data: Dict[str, Any] = self._om_retrievals[filename].retrieve_event_data(
            event_id=event_id
        )

        event_data.data = data["detector_data"]
        event_data.photon_energy = data["beam_energy"]
        event_data.clen = data["detector_distance"]

        if filename in self._peak_lists.keys():
            event_data.peaks = self._peak_lists[filename][event_id]
        elif filename in self._peakfinders.keys():
            if event_data.data is not None:
                peak_list: OmPeakList = self._peakfinders[filename].find_peaks(
                    detector_data=event_data.data
                )
                event_data.peaks = PeakList(
                    num_peaks=peak_list.num_peaks,
                    fs=peak_list.fs,
                    ss=peak_list.ss,
                )
            else:
                raise RuntimeError("Cannot perform peakfinding, data is None")

        return event_data

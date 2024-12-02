"""
Frame retrieval base classes.
"""

import numpy.typing

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


@dataclass
class PeakList:
    """
    A data class which stores information about positions of detected peaks in a
    detector data frame.

    Attributes:

        num_peaks: The number of peaks that were detected in the data frame.

        fs: A list of fractional fs indexes that locate the detected peaks in the data
            frame.

        ss: A list of fractional ss indexes that locate the detected peaks in the data
            frame.
    """

    num_peaks: int
    fs: List[float]
    ss: List[float]


@dataclass
class EventData:
    """
    A data class which stores data from a single event.

    Attributes:

        data: Detector data as a numpy array.

        source: Source of the detector data, for example image filename and event index
            for events extracted from a stream file.

        peaks: A [PeakList][cheetah.frame_retrieval.base.PeakList] dictionary containing
            information about detected peaks.

        photon_energy: Photon energy in eV.

        clen: Detector distance in meters.

        crystals: A list of [PeakList][cheetah.frame_retrieval.base.PeakList] dictionaries
            containing information about predicted reflections for each indexed crystal.
    """

    data: Optional[numpy.typing.NDArray[Any]] = None
    source: Optional[str] = None
    peaks: Optional[PeakList] = None
    photon_energy: Optional[float] = None
    clen: Optional[float] = None
    crystals: Optional[List[PeakList]] = None


class CheetahFrameRetrieval(ABC):
    """
    See documentation of the `__init__` function.
    """

    @abstractmethod
    def __init__(self, sources: List[str], parameters: Dict[str, Any]) -> None:
        """
        Base class for Cheetah Frame Retrieval classes.

        Frame Retrieval classes retrieve event data from various sources. This class is
        a base class from which every Frame Retrieval class should inherit. All its
        methods are abstract. Each derived class must provide its own methods that
        implement data retrieval from a specific data source.

        Arguments:

            sources: A list of strings describing the data event sources.

            parameters: A dictionary containing frame retrieval parameters specific for
                each derived class.
        """
        pass

    @abstractmethod
    def get_event_list(self) -> List[str]:
        """
        Get the list of events.

        This function returns a list of all event IDs which can be retrieved from the
        sources list.

        Returns:

            A list of all event IDs.
        """
        pass

    @abstractmethod
    def get_data(self, event_index: int) -> EventData:
        """
        Get all available frame data for a requested event.

        This function retrieves all available data related to the requested event.

        Arguments:

            event_index: Index of the event in the event list.

        Returns:

            A [EventData][cheetah.frame_retrieval.base.EventData] dictionary
            containing all available data related to the requested event.
        """
        pass

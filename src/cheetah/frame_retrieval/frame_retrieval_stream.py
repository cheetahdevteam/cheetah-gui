"""
Frame retrieval from CrystFEL stream files.
"""
import logging
import pathlib
import subprocess
from typing import Any, Dict, List, TextIO, Tuple, Union, Optional

import h5py  # type: ignore

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.frame_retrieval.base import (
    CheetahFrameRetrieval,
    TypeEventData,
    TypePeakList,
)
from cheetah.utils.logging import log_subprocess_run_output

from om.algorithms.generic import Binning, BinningPassthrough
from om.data_retrieval_layer import OmEventDataRetrieval
from om.lib.geometry import GeometryInformation
from om.lib.parameters import MonitorParameters

logger: logging.Logger = logging.getLogger(__name__)


class _TypeStreamEvent(TypedDict):
    # A dictionary used internally to store information about a single data event in a
    # stream file. offset is a byte offset to the event chunk in the stream file.

    filename: str
    offset: int


class _TypeChunkData(TypedDict, total=False):
    # A dictionary used internally to store data extracted from a stream file chunk.

    image_filename: str
    event: Optional[int]
    om_event_id: str
    om_source: str
    om_config: str
    photon_energy: float
    clen: float
    peaks: TypePeakList
    crystals: List[TypePeakList]


class StreamRetrieval(CheetahFrameRetrieval):
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, sources: List[str], parameters: Dict[str, Any]):
        """
        Frame Retrieval from CrystFEL stream files.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This class implements Cheetah Frame Retrieval from CrystFEL stream files. The
        sources required by this Frame Retrieval class are the names of the stream
        files.

        Arguments:

            sources: A list of stream files.

            parameters: A dictionary containing configuration parameters for data
                retrieval from stream files. No additional parameters are required to
                retrieve data from stream files.
        """
        self._streams: Dict[str, TextIO] = {}
        self._events: List[_TypeStreamEvent] = []
        self._om_retrievals: Dict[Tuple[str, str], OmEventDataRetrieval] = {}
        filename: str
        for filename in sources:
            offsets: List[int] = self._get_index(pathlib.Path(filename))
            if len(offsets) == 0:
                logger.info(f"No events found in {filename}.")
                continue
            self._streams[filename] = open(filename, "r")
            self._events.extend(
                [{"filename": filename, "offset": offset} for offset in offsets]
            )
        self._hdf5_data_path: str = self._get_hdf5_data_path()
        self._num_events: int = len(self._events)

    def _get_hdf5_data_path(self) -> str:
        # Tries to find hdf5 data path in the geometry file part of the source stream
        # files. If the path is not found in any of the source files returns an empty
        # string.
        stream: TextIO
        for stream in self._streams.values():
            reading_geometry: bool = False
            for line in stream:
                if line.startswith("----- End geometry file -----"):
                    reading_geometry = False
                    continue
                elif reading_geometry:
                    if line.startswith("data ="):
                        return line.split()[-1]
                elif line.startswith("----- Begin geometry file -----"):
                    reading_geometry = True
        return ""

    def _get_index(self, stream_filename: pathlib.Path) -> List[int]:
        # Checks if an up-to-date index file already exists, if not runs grep in a
        # subrocess to create new index. Returns offsets of each chunk in the stream
        # file read from the index file.
        stream_mtime: float = stream_filename.stat().st_mtime
        index_filename: pathlib.Path = (
            stream_filename.parent / f"{stream_filename.stem}-index.txt"
        )
        if index_filename.is_file():
            fh: TextIO
            with open(index_filename, "r") as fh:
                index_mtime: float = float(fh.readline().split("=")[1])
                offsets: List[int] = [int(line) for line in fh]
            if index_mtime >= stream_mtime:
                logger.info(f"Loading chunk offsets from {index_filename}.")
                return offsets
            else:
                logger.info(
                    f"Index file {index_filename} is outdated, creating new index."
                )
        else:
            logger.info(f"Creating new index.")

        command: str = (
            f"grep --byte-offset 'Begin chunk' {stream_filename} "
            "| awk '{print $1}' FS=':'"
        )
        logger.info(f"Running command: {command}")
        output: subprocess.CompletedProcess = subprocess.run(
            command, shell=True, capture_output=True
        )
        log_subprocess_run_output(output, logger, only_errors=True)

        offsets: List[int] = [int(line) for line in output.stdout.split()]
        try:
            with open(index_filename, "w") as fh:
                fh.write(f"stream_mtime={stream_filename.stat().st_mtime}\n")
                fh.write(output.stdout.decode())
                logger.info(f"Writing new index file {index_filename}.")
        except PermissionError as e:
            logger.warning(f"Couldn't write index file {index_filename}:\n{e}")

        return offsets

    def _parse_chunk(self, event: _TypeStreamEvent) -> _TypeChunkData:
        # Parses stream chunk and returns chunk data.
        chunk_data: _TypeChunkData = {}
        stream: TextIO = self._streams[event["filename"]]
        stream.seek(event["offset"])

        chunk_data["peaks"] = {"num_peaks": 0, "fs": [], "ss": []}
        chunk_data["crystals"] = []
        reading_peaks: bool = False
        reading_reflections: bool = False

        line: str = stream.readline()
        while not line.startswith("----- End chunk -----"):
            if line.startswith("Image filename:"):
                chunk_data["image_filename"] = line.split(":")[-1].strip()
            elif line.startswith("Event:"):
                try:
                    chunk_data["event"] = int(line.split("//")[-1])
                except ValueError:
                    chunk_data["event"] = None
            elif line.startswith("header/str/event_id"):
                chunk_data["om_event_id"] = "=".join(line.split("=")[1:]).strip()
            elif line.startswith("header/str/source"):
                chunk_data["om_source"] = "=".join(line.split("=")[1:]).strip()
            elif line.startswith("header/str/configuration_file"):
                chunk_data["om_config"] = "=".join(line.split("=")[1:]).strip()
            elif line.startswith("photon_energy_eV"):
                chunk_data["photon_energy"] = float(line.split("=")[-1])
            elif line.startswith("average_camera_length"):
                chunk_data["clen"] = float(line.split()[-2])
            elif line.startswith("End of peak list"):
                reading_peaks = False
            elif reading_peaks:
                split_items: List[str] = line.split()
                chunk_data["peaks"]["num_peaks"] += 1
                chunk_data["peaks"]["fs"].append(float(split_items[0]))
                chunk_data["peaks"]["ss"].append(float(split_items[1]))
            elif line.startswith("  fs/px   ss/px"):
                reading_peaks = True
            elif line.startswith("End of reflections"):
                reading_reflections = False
            elif reading_reflections:
                split_items = line.split()
                chunk_data["crystals"][-1]["num_peaks"] += 1
                chunk_data["crystals"][-1]["fs"].append(float(split_items[7]))
                chunk_data["crystals"][-1]["ss"].append(float(split_items[8]))
            elif line.startswith("   h    k    l"):
                reading_reflections = True
                chunk_data["crystals"].append({"num_peaks": 0, "fs": [], "ss": []})
            line = stream.readline()

        return chunk_data

    def _initialize_binning(self, monitor_parameters: MonitorParameters) -> None:
        # Initializes binning algorithm to be applied to all frames.
        self._geometry_information = GeometryInformation.from_file(
            geometry_filename=monitor_parameters.get_parameter(
                group="crystallography",
                parameter="geometry_file",
                parameter_type=str,
                required=True,
            ),
        )
        if monitor_parameters.get_parameter(
            group="crystallography",
            parameter="post_processing_binning",
            parameter_type=bool,
            default=False,
        ):
            self._binning: Union[Binning, BinningPassthrough] = Binning(
                parameters=monitor_parameters.get_parameter_group(group="binning"),
                layout_info=self._geometry_information.get_layout_info(),
            )
        else:
            self._binning = BinningPassthrough(
                layout_info=self._geometry_information.get_layout_info()
            )

    def get_event_list(self) -> List[str]:
        """
        Get the list of events from stream files.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function returns a list of all event IDs which can be retrieved from the
        list of stream files. Event IDs for events retrieved from stream files are
        constructed by joining the following elements separated by the "//" symbol:

        * The name of the stream file.

        * The byte offset, within the file, to the corresponding chunk.

        Returns:

            A list of event IDs.
        """
        event: _TypeStreamEvent
        return [f"{event['filename']} // {event['offset']}" for event in self._events]

    def get_data(self, event_index: int) -> TypeEventData:
        """
        Get all available frame data for a requested event.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function retrieves all available data related to the requested event in
        the CrystFEL stream file. It tries to extract detector data either from HDF5
        file or using OM frame retrieval (if the data was initially streamed from OM to
        CrystFEL for indexing). Additionally, it extracts photon energy in eV, detector
        distance in meters, and a list of detected peaks and predicted reflection
        positions for each of the indexed crystals from the stream file itself.

        Arguments:

            event_index: Index of the event in the list of events.

        Returns:

            A [TypeEventData][cheetah.frame_retrieval.base.TypeEventData] dictionary
            containing all available data related to the requested event.
        """
        event_data: TypeEventData = {}
        chunk_data: _TypeChunkData = self._parse_chunk(self._events[event_index])
        if (
            "om_source" in chunk_data
            and "om_config" in chunk_data
            and "om_event_id" in chunk_data
        ):
            if (
                chunk_data["om_source"],
                chunk_data["om_config"],
            ) not in self._om_retrievals:
                try:
                    monitor_params: MonitorParameters = MonitorParameters(
                        config=chunk_data["om_config"]
                    )
                    self._om_retrievals[
                        (chunk_data["om_source"], chunk_data["om_config"])
                    ] = OmEventDataRetrieval(
                        source=chunk_data["om_source"],
                        monitor_parameters=monitor_params,
                    )
                    if len(self._om_retrievals) == 1:
                        # Initialize binning algorithm once to be applied to all frames
                        self._initialize_binning(monitor_params)
                except Exception as e:
                    logger.exception(
                        f"Couldn't initialize OM frame retrieval from "
                        f"{chunk_data['om_source']} source using "
                        f"{chunk_data['om_config']} config file:"
                    )

            try:
                om_data: Dict[str, Any] = self._om_retrievals[
                    (chunk_data["om_source"], chunk_data["om_config"])
                ].retrieve_event_data(event_id=chunk_data["om_event_id"])
                event_data["data"] = self._binning.bin_detector_data(
                    data=om_data["detector_data"]
                )
                event_data["source"] = chunk_data["om_event_id"]
            except Exception as e:
                logger.exception(
                    f"Couldn't extract image data for event id "
                    f"{chunk_data['om_event_id']}:"
                )

        elif chunk_data["image_filename"] and chunk_data["event"] is not None:
            try:
                h5_file: Any
                with h5py.File(chunk_data["image_filename"]) as h5_file:
                    event_data["data"] = h5_file[self._hdf5_data_path][
                        chunk_data["event"]
                    ]
                event_data[
                    "source"
                ] = f"{chunk_data['image_filename']} // {chunk_data['event']}"
            except:
                logger.exception(
                    f"Couldn't extract image data from {chunk_data['image_filename']},"
                    f" event //{chunk_data['event']}"
                )

        event_data["photon_energy"] = chunk_data["photon_energy"]
        event_data["clen"] = chunk_data["clen"]
        event_data["peaks"] = chunk_data["peaks"]
        event_data["crystals"] = chunk_data["crystals"]

        return event_data

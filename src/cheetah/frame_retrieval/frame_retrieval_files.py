"""
Frame retrieval from files.
"""
import logging
import h5py  # type: ignore

from typing import Any, Dict, List, Optional

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.frame_retrieval.base import CheetahFrameRetrieval, TypeEventData

logger: logging.Logger = logging.getLogger(__name__)


class _TypeH5Event(TypedDict):
    # A dictionary used internally to store information about a single data event in an
    # HDF5 file. For multi-event files index is the index of the event in the dataset,
    # for single-event files index is -1.

    filename: str
    index: int


class H5FilesRetrieval(CheetahFrameRetrieval):
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, sources: List[str], parameters: Dict[str, Any]):
        """
        Frame Retrieval from HDF5 files.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This class implements Cheetah Frame Retrieval from single- or multi-event HDF5
        files. The sources required by this Frame Retrieval class are the names of the
        HDF5 files. The only parameter which is required by this class to retrieve
        detector data is 'hdf5_data_path'. It can additionaly retrieve detected peaks,
        photon energy and detector distance if corresponding parameters are provided.

        Arguments:

            sources: A list of HDF5 files.

            parameters: A dictionary containing configuration parameters for data
                retrieval from HDF5 files.

                The following parameters are required:

                * `hdf5_data_path': The internal HDF5 path to the dataset where
                  detector data is stored.

                The following parameters are optional:

                * `hdf5_peaks_path`: The internal HDF5 path to the dataset where
                  detected peaks are stored. If this parameter is not provided detected
                  peaks data won't be retrieved.

                * `photon_energy_path`: The internal HDF5 path to the dataset where
                  photon energy in eV is stored. If this parameter is not provided
                  photon energy won't be retrieved.

                * `clen_path`: The internal HDF5 path to the dataset where detector
                  distance in mm is stored. If this parameters is not provided
                  detector distance won't be retrieved.
        """
        self._hdf5_data_path: str = parameters["hdf5_data_path"]
        if "hdf5_peaks_path" in parameters.keys():
            self._hdf5_peaks_path: Optional[str] = parameters["hdf5_peaks_path"]
        else:
            self._hdf5_peaks_path = None
        if "photon_energy_path" in parameters.keys():
            self._photon_energy_path: Optional[str] = parameters["photon_energy_path"]
        else:
            self._photon_energy_path = None
        if "clen_path" in parameters.keys():
            self._clen_path: Optional[str] = parameters["clen_path"]
        else:
            self._clen_path = None

        self._multi_event_files: Dict[str, Any] = {}
        self._events: List[_TypeH5Event] = []
        filename: str
        for filename in sources:
            fh: Any = h5py.File(filename, "r")
            if self._hdf5_data_path not in fh:
                logger.warning(
                    f"Could not find {self._hdf5_data_path} dataset in {filename}. "
                    f"Skipping this file."
                )
                continue
            data = fh[self._hdf5_data_path]
            if len(data.shape) == 2:
                # Close single-event files to let Cheetah update them while the viewer
                # is running
                self._events.append({"filename": filename, "index": -1})
                fh.close()
            else:
                i: int
                self._events.extend(
                    [{"filename": filename, "index": i} for i in range(data.shape[0])]
                )
                # Keep multi-event files open
                self._multi_event_files[filename] = fh

        self._num_events: int = len(self._events)

    def get_event_list(self) -> List[str]:
        """
        Get the list of events from HDF5 files.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function returns a list of all event IDs which can be retrieved from the
        list of source files. Event IDs for events retrieved from HDF5 files are
        constructed by joining the following elements separated by the "//" symbol:

        * The name of the HDF5 file.

        * The index, withing the file, of the corresponding data frame for multi-event
          files, or "-1" for single-event files.

        Returns:

            A list of event IDs.
        """
        event: _TypeH5Event
        return [f"{event['filename']} // {event['index']}" for event in self._events]

    def get_data(self, event_index: int) -> TypeEventData:
        """
        Get all available frame data for a requested event.

        This method overrides the corresponding method of the base class: please also
        refer to the documentation of that class for more information.

        This function retrieves all available data related to the requested event
        stored in the HDF5 file. Depending on the configuration parameters provided
        when HDF5 Frame Retrieval is initialized, it either retrieves only detector
        data or, additionally, any of the following:

        * The list of detected peaks, if `hdf5_peaks_path` parameter is provided and
          the corresponding dataset can be found in the input file.

        * Photon energy in eV, if `photon_energy_path` parameter is provided and the
          corresponding dataset can be found in the input file.

        * Detector distance in m, if `clen_path` parameter is provided and the
          corresponding dataset can be found in the input file.

        Arguments:

            event_index: Index of the event in the list of events.

        Returns:

            A [TypeEventData][cheetah.frame_retrieval.base.TypeEventData] dictionary
            containing all available data related to the requested event.
        """
        event_data: TypeEventData = {}
        filename: str = self._events[event_index]["filename"]
        index: int = self._events[event_index]["index"]

        if index == -1:
            fh: Any = h5py.File(filename)
            event_data["data"] = fh[self._hdf5_data_path][()]
            fh.close()
        else:
            event_data["data"] = self._multi_event_files[filename][
                self._hdf5_data_path
            ][index]

            if self._hdf5_peaks_path:
                if self._hdf5_peaks_path not in self._multi_event_files[filename]:
                    logger.warning(
                        f"Peaks dataset {self._hdf5_peaks_path} not found in "
                        f"{filename}."
                    )
                else:
                    num_peaks: int = self._multi_event_files[filename][
                        self._hdf5_peaks_path
                    ]["nPeaks"][index]
                    fs: List[float] = self._multi_event_files[filename][
                        self._hdf5_peaks_path
                    ]["peakXPosRaw"][index][:num_peaks]
                    ss: List[float] = self._multi_event_files[filename][
                        self._hdf5_peaks_path
                    ]["peakYPosRaw"][index][:num_peaks]

                    event_data["peaks"] = {"num_peaks": num_peaks, "fs": fs, "ss": ss}

            if self._photon_energy_path:
                if self._photon_energy_path not in self._multi_event_files[filename]:
                    logger.warning(
                        f"Photon energy dataset {self._photon_energy_path} not found "
                        f"in {filename}."
                    )
                else:
                    event_data["photon_energy"] = self._multi_event_files[filename][
                        self._photon_energy_path
                    ][index]

            if self._clen_path:
                if self._clen_path not in self._multi_event_files[filename]:
                    logger.warning(
                        f"Detector distance dataset {self._clen_path} not found "
                        f"in {filename}."
                    )
                else:
                    event_data["clen"] = (
                        self._multi_event_files[filename][self._clen_path][index] * 1e-3
                    )

        return event_data

"""
Peakogram GUI.

This module contains Cheetah peakogram GUI.
"""
import pathlib
import sys
from typing import Any, Dict, List, Union, cast

import click  # type: ignore

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

import numpy
import pyqtgraph  # type: ignore
from cheetah import __file__ as cheetah_src_path
from cheetah.utils.file_reader_base import FileReader
from numpy.typing import NDArray
from PyQt5 import QtCore, QtGui, QtWidgets  # type: ignore

from om.lib.geometry import GeometryInformation


class _TypePeak(TypedDict):
    # This typed dictionary is used internally to store peak radius and maximum pixel
    # intensity used to construct a peakogram.
    radius: float
    intensity: float


class _TypePeakogramData(TypedDict):
    # This typed dictionary is used internally to store peakogram data.
    peakogram: NDArray[numpy.float_]
    npeaks: int


class _PeaksReader(FileReader):
    # This class is used interntally to read data from a list of peaks.txt files
    # written by Cheetah. It constructs a peakogram and transmits it to the main
    # Peakogram GUI every 2 seconds.

    def __init__(
        self,
        filenames: List[str],
        parameters: Dict[str, Any],
    ) -> None:
        super(_PeaksReader, self).__init__(
            filenames, parameters, output_emit_interval=2000, sleep_timeout=10000
        )

        self._radius_pixelmap: NDArray[numpy.float_] = GeometryInformation(
            geometry_filename=parameters["geometry"]
        ).get_pixel_maps()["radius"]

        self._peak_list: List[_TypePeak] = []
        self._npeaks: int = 0

        self._peakogram_radius_bin_size: float = parameters["radius_bin_size"]
        self._peakogram_intensity_bin_size: float = parameters["intensity_bin_size"]
        self._peakogram: NDArray[numpy.float_] = numpy.zeros((1, 1))

    def _process_line(self, line: str) -> None:
        # Processes a line from the input file adding information to the peak list.
        if line.startswith("event_id"):
            return
        split_items: List[str] = line.split(",")
        if len(split_items) > 7:
            peak_fs: float = float(split_items[-6])
            peak_ss: float = float(split_items[-5])

            self._peak_list.append(
                {
                    "radius": self._radius_pixelmap[
                        int(round(peak_ss)), int(round(peak_fs))
                    ],
                    "intensity": float(split_items[-2]),
                }
            )

    def _prepare_output(self) -> Union[None, Dict[str, Any]]:
        # Prepares output dictionary from accumulated data. If there're new peaks in
        # the peak list adds them to the peakogram and returns the resulting peakogram
        # and the total number of loaded peaks.
        if len(self._peak_list) == 0:
            return None

        self._npeaks += len(self._peak_list)

        peak: _TypePeak
        peaks_max_intensity: float = max(
            (peak["intensity"] for peak in self._peak_list)
        )
        peakogram_max_intensity: float = (
            self._peakogram.shape[1] * self._peakogram_intensity_bin_size
        )
        if peaks_max_intensity > peakogram_max_intensity:
            self._peakogram = numpy.concatenate(  # type: ignore
                (
                    self._peakogram,
                    numpy.zeros(
                        (
                            self._peakogram.shape[0],
                            int(
                                (peaks_max_intensity - peakogram_max_intensity)
                                // self._peakogram_intensity_bin_size
                                + 1
                            ),
                        )
                    ),
                ),
                axis=1,
            )
        peaks_max_radius: float = max((peak["radius"] for peak in self._peak_list))
        peakogram_max_radius: float = (
            self._peakogram.shape[0] * self._peakogram_radius_bin_size
        )
        if peaks_max_radius > peakogram_max_radius:
            self._peakogram = numpy.concatenate(  # type: ignore
                (
                    self._peakogram,
                    numpy.zeros(
                        (
                            int(
                                (peaks_max_radius - peakogram_max_radius)
                                // self._peakogram_radius_bin_size
                                + 1
                            ),
                            self._peakogram.shape[1],
                        )
                    ),
                ),
                axis=0,
            )
        for peak in self._peak_list:
            self._peakogram[
                int(peak["radius"] // self._peakogram_radius_bin_size),
                int(peak["intensity"] // self._peakogram_intensity_bin_size),
            ] += 1

        self._peak_list = []
        return {"peakogram": self._peakogram, "npeaks": self._npeaks}


class PeakogramGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    _stop_reader_thread: Any = QtCore.pyqtSignal()

    def __init__(
        self,
        input_files: List[str],
        geometry_filename: str,
        radius_bin_size: float,
        intensity_bin_size: float,
    ) -> None:
        """
        Cheetah Peakogram GUI.

        This class implements Cheetah peakogram GUI. The GUI starts a file reader in a
        separate thread, which continuously reads peaks from the list of input peak
        files, constructs a peakogram and transmits it to the main thread every 2
        seconds. The reader continues to update the peakogram as long as the input peak
        files are being written by Cheetah.

        Arguments:

            input_files: A list of input peak.txt files.

            geometry_filename: The name of CrystFEL geometry file.

            radius_bin_size: The size of the peakogram bin along the 'radius' direction
                in pixels.

            intensity_bin_size: The size of the peakogram bin along the 'intensity'
                direction in ADU.
        """
        super(PeakogramGui, self).__init__()
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self._peakogram_radius_bin_size: float = radius_bin_size
        self._peakogram_intensity_bin_size: float = intensity_bin_size
        self._peakogram: NDArray[numpy.float_] = numpy.zeros((1, 1))

        self._peakogram_plot_widget = pyqtgraph.PlotWidget(
            title="Peakogram", lockAspect=False
        )
        self._peakogram_plot_widget.showGrid(x=True, y=True)
        self._peakogram_plot_widget.setLabel(
            axis="left", text="Peak maximum intensity, AU"
        )
        self._peakogram_plot_widget.setLabel(
            axis="bottom",
            text="Resolution, pixels",
        )
        self._peakogram_plot_image_view = pyqtgraph.ImageView(
            view=self._peakogram_plot_widget.getPlotItem(),
        )
        self._peakogram_plot_image_view.ui.roiBtn.hide()
        self._peakogram_plot_image_view.ui.menuBtn.hide()
        self._peakogram_plot_image_view.view.invertY(False)
        self._peakogram_plot_image_view.setColorMap(pyqtgraph.colormap.get("CET-I1"))

        self.setCentralWidget(self._peakogram_plot_image_view)
        self.resize(600, 600)
        self.show()

        parameters: Dict[str, Any] = {
            "geometry": geometry_filename,
            "radius_bin_size": self._peakogram_radius_bin_size,
            "intensity_bin_size": self._peakogram_intensity_bin_size,
        }

        self._peak_reader_thread: Any = QtCore.QThread(parent=self)
        self._peak_reader: _PeaksReader = _PeaksReader(
            input_files,
            parameters,
        )
        self._peak_reader.moveToThread(self._peak_reader_thread)
        self._peak_reader_thread.started.connect(self._peak_reader.start)
        self._peak_reader.output.connect(self._update_peakogram)
        self._stop_reader_thread.connect(self._peak_reader.stop)
        self._peak_reader_thread.start()

    def _update_peakogram(self, data: _TypePeakogramData) -> None:
        # Updates the peakogram.
        self._peakogram_plot_widget.setTitle(
            f"Peakogram: {data['npeaks']} peaks loaded."
        )
        peakogram: NDArray[numpy.float_] = data["peakogram"]
        peakogram[numpy.where(peakogram == 0)] = numpy.nan
        self._peakogram_plot_image_view.setImage(
            numpy.log(peakogram),
            pos=(0, 0),
            scale=(
                self._peakogram_radius_bin_size,
                self._peakogram_intensity_bin_size,
            ),
            autoRange=False,
            autoLevels=False,
            autoHistogramRange=False,
        )
        self._peakogram_plot_widget.setAspectLocked(lock=False)

    def closeEvent(self, event: Any) -> None:
        """
        Stop the file reader, quit the reader thread and accept.

        This function is called when the GUI window is closed. It cleanly stops the
        reader thread before closing.
        """
        self._stop_reader_thread.emit()
        self._peak_reader_thread.quit()
        self._peak_reader_thread.wait()
        event.accept()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument(  # type: ignore
    "input_files",
    nargs=-1,
    type=click.Path(exists=True),
    required=True,
    metavar="INPUT_FILE(S)",
)
@click.option(  # type: ignore
    "--geometry",
    "-g",
    "geometry_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=True,
    help="CrystFEL geometry file",
)
@click.option(  # type: ignore
    "--radius-bin-size",
    "-r",
    "radius_bin_size",
    type=float,
    default=3.0,
    help="radius bin size in pixels, default: 3",
)
@click.option(  # type: ignore
    "--intensity-bin-size",
    "-i",
    "intensity_bin_size",
    type=float,
    default=10.0,
    help="intensity bin size in ADU, default: 10",
)
def main(
    input_files: List[str],
    geometry_filename: str,
    radius_bin_size: float,
    intensity_bin_size: float,
) -> None:
    """
    Cheetah Peakogram GUI. The GUI displays the peakogram using the peaks from
    peaks.txt files written by Cheetah. It keeps updating the peakogram as long as the
    input files are being written. One or several peaks.txt files must be provided as
    INPUT_FILE(S). Additionally, CrystFEL geometry file ('--geometry' / '-g') must also
    be provided.
    """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = PeakogramGui(
        input_files, geometry_filename, radius_bin_size, intensity_bin_size
    )
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

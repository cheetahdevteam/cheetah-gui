"""
Hitrate GUI.

This module contains Cheetah hitrate GUI.
"""
import click  # type: ignore
import numpy
import pathlib
import sys
from sortedcontainers import SortedList  # type: ignore
from scipy.ndimage.filters import uniform_filter1d  # type: ignore
from typing import Any, List, Dict, Union, cast

from numpy.typing import NDArray

from PyQt5 import QtGui, QtCore, QtWidgets  # type: ignore
import pyqtgraph  # type: ignore

from cheetah.utils.file_reader_base import FileReader
from cheetah import __file__ as cheetah_src_path


class _FramesReader(FileReader):
    # This class is used interntally to read data from a list of frames.txt files
    # written by Cheetah. It constructs a hitrate plot for each file and transmits
    # them to the main Hitrate GUI every 2 seconds.

    def __init__(
        self,
        filenames: List[str],
        parameters: Dict[str, Any],
    ) -> None:
        super(_FramesReader, self).__init__(
            filenames, parameters, output_emit_interval=2000, sleep_timeout=10000
        )
        self._running_average_window_size: int = parameters[
            "running_average_window_size"
        ]
        self._events: List[SortedList] = [
            SortedList(key=lambda e: e["timestamp"]) for fn in self._filenames
        ]
        self._num_new_events: int = 0

    def _process_line(self, line: str) -> None:
        # Processes a line from the input file adding information to the event list.
        if line.startswith("# timestamp"):
            return

        split_items: List[str] = line.split(",")
        if len(split_items) > 2:
            self._events[self._current].add(
                {"timestamp": float(split_items[0]), "hit": int(split_items[2])}
            )
            self._num_new_events += 1

    def _prepare_output(self) -> Union[None, Dict[str, Any]]:
        # Prepares output dictionary from accumulated data.
        if self._num_new_events == 0:
            return None
        index: int
        hitrate: Dict[str, Any] = {}
        for index in range(len(self._filenames)):
            hitrate[self._filenames[index]] = (
                uniform_filter1d(
                    [event["hit"] for event in self._events[index]],
                    size=self._running_average_window_size,
                    output=float,
                )
                * 100
            )
        self._num_new_events = 0
        return hitrate


class HitrateGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    _stop_reader_thread: Any = QtCore.pyqtSignal()

    def __init__(
        self,
        input_files: List[str],
        running_average_window_size: int,
    ):
        """
        Cheetah hitrate GUI.

        This class implements Cheetah hitrate GUI. The GUI starts a file reader in a
        separate thread, which continuously reads processed events information from the
        list of input frames.txt files, sorts event by timestamp and calculates running
        average hitrate. The reader continues to update the hitrate plots as long as
        the input frames.txt files are being written by Cheetah.

        Arguments:

            input_files: A list of input peak.txt files.

            running_average_window_size: The size of the running window used to compute
                the average hit rate.
        """
        super(HitrateGui, self).__init__()
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self._running_average_window_size: float = running_average_window_size

        self._hitrate_plot_widget = pyqtgraph.PlotWidget(
            title="Hitrate vs Events", lockAspect=False
        )
        self._hitrate_plot_widget.showGrid(x=True, y=True)
        self._hitrate_plot_widget.setLabel(axis="left", text="Hitrate, %")
        self._hitrate_plot_widget.setLabel(
            axis="bottom",
            text="Events",
        )
        self._hitrate_plot_widget.addLegend()
        filename: str
        self._hitrate_plots: Dict[str, Any] = {}
        self._pens: Dict[str, Any] = {}
        index: int = 0
        for filename in input_files:
            self._hitrate_plots[filename] = self._hitrate_plot_widget.plot(
                name=filename.split("/")[-2]
            )
            self._pens[filename] = pyqtgraph.mkPen(
                pyqtgraph.intColor(index, len(input_files)), width=2
            )
            index += 1
        self.setCentralWidget(self._hitrate_plot_widget)
        self.resize(600, 500)
        self.show()

        parameters: Dict[str, Any] = {
            "running_average_window_size": running_average_window_size
        }

        self._frame_reader_thread: Any = QtCore.QThread(parent=self)
        self._frame_reader: _FramesReader = _FramesReader(
            input_files,
            parameters,
        )
        self._frame_reader.moveToThread(self._frame_reader_thread)
        self._frame_reader_thread.started.connect(self._frame_reader.start)
        self._frame_reader.output.connect(self._update_plots)
        self._stop_reader_thread.connect(self._frame_reader.stop)
        self._frame_reader_thread.start()

    def _update_plots(self, data: Dict[str, NDArray[numpy.float_]]) -> None:
        # Updates hitrate plots.
        filename: str
        for filename in data:
            self._hitrate_plots[filename].setData(
                data[filename], pen=self._pens[filename]
            )

    def closeEvent(self, event: Any) -> None:
        """
        Stop the file reader, quit the reader thread and accept.

        This function is called when the GUI window is closed. It cleanly stops the
        reader thread before closing.
        """
        self._stop_reader_thread.emit()
        self._frame_reader_thread.quit()
        self._frame_reader_thread.wait()
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
    "--running-average-window-size",
    "-s",
    "running_average_window_size",
    type=int,
    default=200,
    help="the size of the running average window to compute hitrate, default: 200",
)
def main(
    input_files: List[str],
    running_average_window_size: int,
) -> None:
    """
    Cheetah Hitrate GUI. The GUI displays the hitrate plot for each of the input
    frame.txt files written by Cheetah. It keeps updating the plots as long as the
    input files are being written. One or several frames.txt files must be provided as
    INPUT_FILE(S).
    """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = HitrateGui(input_files, running_average_window_size)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

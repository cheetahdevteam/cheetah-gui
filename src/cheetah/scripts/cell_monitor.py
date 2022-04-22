"""
Cell Monitor.

This module contains online cell monitor GUI.
"""
import click  # type: ignore
import numpy
import pathlib
import sys
import time
from sortedcontainers import SortedList  # type: ignore
from scipy.ndimage.filters import uniform_filter1d  # type: ignore

from numpy.typing import NDArray
from typing import Any, List, Dict, Union, TextIO

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from PyQt5 import QtGui, QtCore, QtWidgets  # type: ignore
import pyqtgraph  # type: ignore

from cheetah.utils.file_reader_base import FileReader
from cheetah import __file__ as cheetah_src_path


class _TypeCrystal(TypedDict):
    # This typed dictionary is used internally to store information about an indexed
    # crystal.
    timestamp: float
    cell: List[float]
    centering: str
    resolution: float


class _TypeFrame(TypedDict):
    # This typed dictionary is used internally to store indexing result of a frame.
    timestamp: float
    indexed: bool


class _StreamReader(FileReader):
    # This class is used interntally to read data from a list of stream files written
    # by CrystFEL program indexamajig. It accumulates indexing results and transmits
    # them to the main Cell Monitor GUI every 2 seconds.

    def __init__(self, filenames: List[str], parameters: Dict[str, Any]) -> None:
        super(_StreamReader, self).__init__(
            filenames, {}, output_emit_interval=2000, sleep_timeout=2000
        )
        if parameters["skip_to_end"]:
            fh: TextIO
            for fh in self._files:
                fh.seek(0, 2)

        self._frames: List[_TypeFrame] = []
        self._crystals: List[_TypeCrystal] = []

    def _process_line(self, line: str) -> None:
        # Processes a line from the input file adding information to the frame and
        # crystal list.
        if line.startswith("----- Begin chunk -----"):
            self._timestamp: float = time.time()
            self._indexed: bool = False

        elif line.startswith("header/float/timestamp"):
            self._timestamp = float(line.split()[-1])

        elif line.startswith("Cell parameters"):
            self._indexed = True
            item: str
            self._cell = [float(item) for item in line.split()[2:9] if item[0] != "n"]
            # Convert a, b, c to Angstrom:
            i: int
            for i in range(3):
                self._cell[i] *= 10

        elif line.startswith("centering"):
            self._centering = line.split()[-1].strip()

        elif line.startswith("diffraction_resolution_limit"):
            self._resolution = float(line.split()[-2])

        elif line.startswith("--- End crystal"):
            self._crystals.append(
                {
                    "timestamp": self._timestamp,
                    "cell": self._cell,
                    "centering": self._centering,
                    "resolution": self._resolution,
                }
            )

        elif line.startswith("----- End chunk"):
            self._frames.append(
                {"timestamp": self._timestamp, "indexed": self._indexed}
            )

    def _prepare_output(self) -> Union[None, Dict[str, Any]]:
        # Prepares output dictionary with accumulated data.
        if len(self._frames) == 0:
            return None
        output: Dict[str, Any] = {
            "frames": self._frames.copy(),
            "crystals": self._crystals.copy(),
        }
        self._frames = []
        self._crystals = []
        return output


class CellMonitorGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    _stop_reader_thread: Any = QtCore.pyqtSignal()

    def __init__(
        self,
        input_files: List[str],
        running_average_window_size: int,
        skip_to_end: bool,
    ) -> None:
        """
        Cell Monitor GUI.

        This class implements Online Cell Monitor GUI. The GUI start a stream reader in
        a separate thread, which continuously reads indexing results from the list of
        input stream files and transmits them to the main thread every 2 seconds. The
        received information is used to display unit cell parameter distributions and
        the average indexing rate as a function of time.

        Arguments:

            input_files: A list of CrystFEL stream files.

            running_average_window_size: The size of the running window used to compute
                the average indexing rate.
        """
        super(CellMonitorGui, self).__init__()
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self._input_files: List[str] = input_files
        self._running_average_window_size: int = running_average_window_size
        self._skip_to_end: bool = skip_to_end
        self._crystals: SortedList = SortedList(key=lambda d: d["timestamp"])
        self._frames: SortedList = SortedList(key=lambda d: d["timestamp"])

        self._plots_widget: Any = pyqtgraph.GraphicsLayoutWidget(show=True)

        self._plots: List[Any] = []
        self._plots.append(self._plots_widget.addPlot(name="0", title="a"))
        self._plots.append(self._plots_widget.addPlot(name="1", title="b"))
        self._plots.append(self._plots_widget.addPlot(name="2", title="c"))

        self._plots_widget.nextRow()

        self._plots.append(self._plots_widget.addPlot(name="3", title="alpha"))
        self._plots.append(self._plots_widget.addPlot(name="4", title="beta"))
        self._plots.append(self._plots_widget.addPlot(name="5", title="gamma"))

        self._ranges = [[0, 500], [0, 500], [0, 500], [0, 180], [0, 180], [0, 180]]
        self._curves_parameters: Dict[str, Any] = {
            "stepMode": "center",
            "fillLevel": 0,
            "fillOutline": False,
            "brush": (0, 0, 255, 150),
        }
        self._curves_latest_parameters: Dict[str, Any] = {
            "stepMode": "center",
            "fillLevel": 0,
            "fillOutline": False,
            "pen": pyqtgraph.mkPen(color=(0, 255, 0, 100)),
            "brush": (0, 255, 0, 100),
        }
        self._curves: List[Any] = []
        self._curves_latest: List[Any] = []
        i: int
        for i in range(6):
            self._curves.append(self._plots[i].plot())
            self._curves_latest.append(self._plots[i].plot())
            self._plots[i].setMouseEnabled(x=True, y=False)
            self._plots[i].enableAutoRange(x=False, y=True)
            self._plots[i].sigXRangeChanged.connect(self._update_range)
            self._plots[i].setXRange(*self._ranges[i], padding=0)

        legend: Any = pyqtgraph.LegendItem(offset=(0, 70))
        legend.setParentItem(self._plots[2])
        legend.addItem(self._curves[2], "all cells")
        legend.addItem(self._curves_latest[2], "last 10 cells")

        self._indexing_rate_plot_widget: Any = pyqtgraph.PlotWidget()
        self._indexing_rate_plot_widget.setTitle("Indexing Rate vs. Time")
        self._indexing_rate_plot_widget.setLabel(axis="bottom", text="Time, min")
        self._indexing_rate_plot_widget.setLabel(axis="left", text="Indexing Rate, %")
        self._indexing_rate_plot_widget.showGrid(x=True, y=True)
        self._indexing_rate_plot_widget.setYRange(0, 100.0)
        self._indexing_rate_plot: Any = self._indexing_rate_plot_widget.plot(
            tuple(range(-5000, 0)), [0.0] * 5000
        )

        self._delay_widget: Any = QtWidgets.QLabel()

        self._clear_data_button: Any = QtWidgets.QPushButton("Clear data")
        self._clear_data_button.clicked.connect(self._clear_data)

        self._reload_stream_button: Any = QtWidgets.QPushButton("Reload stream")
        self._reload_stream_button.clicked.connect(self._reload_streams)

        self._log_widget: Any = QtWidgets.QPlainTextEdit()
        self._log_widget.setMaximumHeight(150)
        self._log_widget.setReadOnly(True)
        self._log_widget.setLineWrapMode(QtWidgets.QPlainTextEdit.NoWrap)

        buttons_layout: Any = QtWidgets.QHBoxLayout()
        buttons_layout.addWidget(self._clear_data_button)
        buttons_layout.addWidget(self._reload_stream_button)
        buttons_widget: Any = QtWidgets.QWidget()
        buttons_widget.setLayout(buttons_layout)

        layout: Any = QtWidgets.QVBoxLayout()
        layout.addWidget(self._plots_widget)
        layout.addWidget(self._delay_widget)
        layout.addWidget(buttons_widget)
        layout.addWidget(self._log_widget)

        hists_widget: Any = QtWidgets.QWidget()
        hists_widget.setLayout(layout)

        splitter: Any = QtWidgets.QSplitter()
        splitter.addWidget(hists_widget)
        splitter.addWidget(self._indexing_rate_plot_widget)

        main_layout: Any = QtWidgets.QHBoxLayout()
        main_layout.addWidget(splitter)

        central_widget: Any = QtWidgets.QWidget()
        central_widget.setLayout(main_layout)
        self.setCentralWidget(central_widget)

        self.resize(1000, 500)
        self.show()
        self._start_stream_reader()

    def _start_stream_reader(self) -> None:
        # Start stream reader in a separate thread.
        self._stream_reader_thread: Any = QtCore.QThread(parent=self)
        self._stream_reader: _StreamReader = _StreamReader(
            self._input_files, {"skip_to_end": self._skip_to_end}
        )
        self._stream_reader.moveToThread(self._stream_reader_thread)
        self._stream_reader_thread.started.connect(self._stream_reader.start)
        self._stream_reader.output.connect(self._update_data)
        self._stop_reader_thread.connect(self._stream_reader.stop)
        self._stream_reader_thread.start()

    def _stop_stream_reader(self) -> None:
        # Stop stream reader and kill the stream reader thread.
        self._stop_reader_thread.emit()
        self._stream_reader_thread.quit()
        self._stream_reader_thread.wait()

    def _reload_streams(self) -> None:
        # Restart stream reader to reopen input files and read from the beginning.
        self._stop_stream_reader()
        self._start_stream_reader()

    def _clear_data(self) -> None:
        # Clear accumulated data.
        self._crystals = SortedList(key=lambda d: d["timestamp"])
        self._frames = SortedList(key=lambda d: d["timestamp"])

    def _update_data(self, data: Dict[str, Any]) -> None:
        # Update accumulated data.
        self._frames.update(data["frames"])
        self._crystals.update(data["crystals"])
        crystal: _TypeCrystal
        self._write_to_log(
            "".join(
                [
                    "{} cell: {:.2f} {:.2f} {:.2f} A, {:.2f} {:.2f} {:.2f} deg, "
                    "resolution {} A\n".format(
                        crystal["centering"], *crystal["cell"], crystal["resolution"]
                    )
                    for crystal in data["crystals"]
                ]
            )
        )
        self._update_plots()

    def _update_plots(self) -> None:
        # Update cell parameter histograms and indexing rate plot when the new data
        # received.
        frame: _TypeFrame
        indexed: List[bool] = [frame["indexed"] for frame in self._frames]
        num_frames: int = len(self._frames)
        num_indexed: int = sum(indexed)
        num_crystals: int = len(self._crystals)

        print(
            f"Indexed {num_indexed} out of {num_frames} frames: "
            f"{num_crystals} crystals."
        )

        if num_crystals == 0:
            i: int
            for i in range(6):
                self._curves[i].setData([0, 1], [0], **self._curves_parameters)
                self._curves_latest[i].setData(
                    [0, 1], [0], **self._curves_latest_parameters
                )
            return

        timestamp: float = time.time()
        delay: float = timestamp - self._crystals[-1]["timestamp"]
        delay_min: int = int(delay / 60)
        delay_sec: float = delay - delay_min * 60
        self._delay_widget.setText(
            f"Last indexed event: {delay_min} minutes {delay_sec:.2f} seconds ago"
        )
        self._indexing_rate_plot.setData(
            [(frame["timestamp"] - timestamp) / 60 for frame in self._frames],
            uniform_filter1d(
                indexed,
                size=self._running_average_window_size,
                output=float,
            )
            * 100,
        )
        self._update_histograms()

    def _update_histograms(self) -> None:
        # Update cell parameter histograms.
        i: int
        for i in range(6):
            crystal: _TypeCrystal
            y: NDArray[numpy.int_]
            x: NDArray[numpy.float_]
            y, x = numpy.histogram(  # type: ignore
                [crystal["cell"][i] for crystal in self._crystals],
                bins=30,
                range=self._ranges[i],
            )
            with numpy.errstate(divide="ignore", invalid="ignore"):
                self._curves[i].setData(x, y / y.max(), **self._curves_parameters)

            y, x = numpy.histogram(  # type: ignore
                [crystal["cell"][i] for crystal in self._crystals[-10:]],
                bins=90,
                range=self._ranges[i],
            )
            with numpy.errstate(divide="ignore", invalid="ignore"):
                self._curves_latest[i].setData(
                    x, y / y.max(), **self._curves_latest_parameters
                )

    def _update_range(self) -> None:
        # Update histogram range.
        sender: Any = self.sender()
        i: int = int(sender.getViewBox().name)
        self._ranges[i] = self._plots[i].getViewBox().viewRange()[0]
        if len(self._crystals) > 0:
            self._update_histograms()

    def _write_to_log(self, text: str) -> None:
        self._log_widget.moveCursor(QtGui.QTextCursor.End)
        self._log_widget.insertPlainText(text)
        sb = self._log_widget.verticalScrollBar()
        sb.setValue(sb.maximum())

    def closeEvent(self, event: Any) -> None:
        """
        Stop the file reader, quit the reader thread and accept.

        This function is called when the GUI window is closed. It cleanly stops the
        reader thread before closing.
        """
        self._stop_stream_reader()
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
    default=50,
    help="the size of the running average window to compute indexing rate, default: 50",
)
@click.option(  # type: ignore
    "--skip-to-end",
    "-e",
    "skip_to_end",
    is_flag=True,
    default=False,
    help="skip to the end of INPUT_FILE(S) on start-up",
)
def main(
    input_files: List[str], running_average_window_size: int, skip_to_end: bool
) -> None:
    """
    Online Cell Monitor. The monitor displays unit cell parameter distributions and the
    average indexing rate plot using data from a list of CrystFEL stream files provided
    as INPUT_FILE(S). It keeps updating the plots as long as the input files are being
    written. If timestamps are not provided in the input stream files (under
    'header/float/timestamp' tag), the time when the monitor reads the event is used as
    event timestamp.
    """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = CellMonitorGui(input_files, running_average_window_size, skip_to_end)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

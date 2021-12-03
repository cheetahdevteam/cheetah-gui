import click  # type: ignore
import h5py  # type: ignore
import numpy
import numpy.typing
import pathlib
import sys

from om.utils import crystfel_geometry  # type: ignore
from PyQt5 import QtGui, QtCore, QtWidgets, uic  # type: ignore
import pyqtgraph  # type: ignore
from random import randrange
from scipy import constants  # type: ignore
from typing import Any, List, Dict, TextIO, Union, Tuple, TypedDict, cast

from cheetah import __file__ as cheetah_src_path


class TypeEvent(TypedDict):
    """ """

    filename: str
    event: int


class Viewer(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        input_files: List[str],
        hdf5_data_path: str,
        geometry_filename: str,
        hdf5_peaks_path: Union[str, None] = None,
        mask_filename: Union[str, None] = None,
        mask_hdf5_path: Union[str, None] = None,
    ) -> None:
        """ """
        super(Viewer, self).__init__()
        self._ui: Any = uic.loadUi(
            (pathlib.Path(cheetah_src_path) / "../ui_src/viewer.ui").resolve(), self
        )
        self.setWindowTitle(f"Cheetah Viewer")
        self.show()

        self._load_geometry(geometry_filename)
        if hdf5_data_path:
            self._hdf5_data_path: str = hdf5_data_path
        if mask_filename:
            self._mask_filename: str = mask_filename
            if mask_hdf5_path:
                self._mask_hdf5_path: str = mask_hdf5_path
            else:
                self._mask_hdf5_path = "/data/data"

        if self._mask_filename and self._mask_hdf5_path:
            mask_file: Any
            # TODO: check if mask file exists if it's from geometry
            with h5py.File(self._mask_filename) as mask_file:
                if self._mask_hdf5_path not in mask_file:
                    print(
                        f"Dataset {self._mask_hdf5_path} not found in the mask file "
                        f"{self._mask_filename}."
                    )
                    self._ui.show_mask_cb.setEnabled(False)
                else:
                    mask: numpy.typing.NDArray[Any] = (
                        1 - mask_file[self._mask_hdf5_path][()]
                    )
                    mask_img: numpy.typing.NDArray[Any] = numpy.zeros(
                        self._visual_img_shape, dtype=mask.dtype
                    )
                    mask_img[
                        self._visual_pixelmap_y, self._visual_pixelmap_x
                    ] = mask.ravel()
                    self._mask: numpy.typing.NDArray[Any] = numpy.zeros(
                        shape=mask_img.T.shape + (4,)
                    )
                    self._mask[:, :, 2] = mask_img.T
                    self._mask[:, :, 3] = mask_img.T
        else:
            self._ui.show_mask_cb.setEnabled(False)

        self._input_files: Dict[str, Any] = {}
        self._events: List[TypeEvent] = []
        filename: str
        for filename in input_files:
            self._input_files[filename] = h5py.File(filename, "r")
            data = self._input_files[filename][self._hdf5_data_path]
            if len(data.shape) == 2:
                self._events.append({"filename": filename, "event": -1})
            else:
                i: int
                self._events.extend(
                    [{"filename": filename, "event": i} for i in range(data.shape[0])]
                )

        self._num_events = len(self._events)
        self._current_event = 0
        self._hdf5_peaks_path = hdf5_peaks_path
        if not self._hdf5_peaks_path:
            self._ui.show_peaks_cb.setEnabled(False)

        self._data_shape: Tuple[int, int] = (data.shape[-2], data.shape[-1])
        self._frame_data_img: numpy.typing.NDArray[Any] = numpy.zeros(
            self._visual_img_shape, dtype=data.dtype
        )

        pyqtgraph.setConfigOption("background", 0.2)

        self._image_view: Any = self._ui.image_view
        self._image_view.ui.menuBtn.hide()
        self._image_view.ui.roiBtn.hide()
        self._image_view.scene.sigMouseMoved.connect(self._mouse_moved)
        self._image_hist = self._image_view.getHistogramWidget()
        self._image_hist.sigLevelsChanged.connect(self._hist_range_changed)
        self._levels_range: Tuple[Union[int, float], Union[int, float]] = (0, 1)
        self._ui.auto_range_cb.setChecked(True)
        self._ui.auto_range_cb.stateChanged.connect(self._update_image_and_peaks)

        self._ui.min_range_le.editingFinished.connect(self._change_levels)
        self._ui.max_range_le.editingFinished.connect(self._change_levels)

        self._ring_pen: Any = pyqtgraph.mkPen("r", width=2)
        self._peak_canvas: Any = pyqtgraph.ScatterPlotItem()
        self._image_view.getView().addItem(self._peak_canvas)

        self._resolution_rings_in_a: List[float] = [
            10.0,
            6.0,
            4.0,
            3.0,
            2.0,
            1.5,
        ]
        self._resolution_rings_textitems: List[Any] = [
            pyqtgraph.TextItem(
                text="{0}A".format(x), anchor=(0.5, 0.8), color=(0, 255, 0)
            )
            for x in self._resolution_rings_in_a
        ]
        self._resolution_rings_enabled: bool = False
        self._resolution_rings_pen: Any = pyqtgraph.mkPen("g", width=1)
        self._resolution_rings_canvas: Any = pyqtgraph.ScatterPlotItem()
        self._image_view.getView().addItem(self._resolution_rings_canvas)

        self._resolution_rings_regex: Any = QtCore.QRegExp(r"[0-9.,]+")
        self._resolution_rings_validator: Any = QtGui.QRegExpValidator()
        self._resolution_rings_validator.setRegExp(self._resolution_rings_regex)

        self._resolution_rings_check_box: Any = self._ui.show_resolution_rings_cb
        self._resolution_rings_check_box.setEnabled(True)
        self._resolution_rings_lineedit: Any = self._ui.show_resolution_rings_le
        self._resolution_rings_lineedit.setValidator(self._resolution_rings_validator)
        self._resolution_rings_lineedit.setText(
            ",".join(str(x) for x in self._resolution_rings_in_a)
        )
        self._resolution_rings_lineedit.editingFinished.connect(
            self._update_resolution_rings_radii
        )
        self._resolution_rings_lineedit.setEnabled(True)
        self._resolution_rings_check_box.stateChanged.connect(
            self._update_resolution_rings_status
        )

        self._mask_image: Any = pyqtgraph.ImageItem()
        self._mask_image.setZValue(1)
        self._mask_image.setOpacity(0.5)
        self._image_view.getView().addItem(self._mask_image)
        self._ui.show_mask_cb.stateChanged.connect(self._update_mask_image)

        self._ui.show_peaks_cb.stateChanged.connect(self._update_image_and_peaks)
        self._ui.next_button.clicked.connect(self._next_pattern)
        self._ui.previous_button.clicked.connect(self._previous_pattern)
        self._ui.random_button.clicked.connect(self._random_pattern)
        self._ui.play_button.clicked.connect(self._play)
        self._ui.pause_button.clicked.connect(self._pause)
        self._ui.shuffle_button.setCheckable(True)
        self._ui.shuffle_button.clicked.connect(self._shuffle_changed)

        self._update_image_and_peaks()
        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._next_pattern)
        self._ui.pause_button.setEnabled(False)

    def _load_geometry(self, geometry_filename: str) -> None:
        self._geometry: crystfel_geometry.TypeDetector
        beam: crystfel_geometry.TypeBeam
        self._geometry, beam, __ = crystfel_geometry.load_crystfel_geometry(
            filename=geometry_filename
        )
        self._pixelmaps: crystfel_geometry.TypePixelMaps = (
            crystfel_geometry.compute_pix_maps(geometry=self._geometry)
        )

        first_panel: crystfel_geometry.TypePanel = list(
            self._geometry["panels"].keys()
        )[0]
        self._pixel_size: float = self._geometry["panels"][first_panel]["res"]
        self._clen_from: str = self._geometry["panels"][first_panel]["clen_from"]
        if self._clen_from == "":
            self._clen: float = self._geometry["panels"][first_panel]["clen"]
        self._coffset: float = self._geometry["panels"][first_panel]["coffset"]
        self._photon_energy_from: str = beam["photon_energy_from"]
        if self._photon_energy_from == "":
            self._photon_energy: float = beam["photon_energy"]
        self._hdf5_data_path = self._geometry["panels"][first_panel]["data"]
        self._mask_filename = self._geometry["panels"][first_panel]["mask_file"]
        self._mask_hdf5_path = self._geometry["panels"][first_panel]["mask"]

        y_minimum: int = (
            2
            * int(max(abs(self._pixelmaps["y"].max()), abs(self._pixelmaps["y"].min())))
            + 2
        )
        x_minimum: int = (
            2
            * int(max(abs(self._pixelmaps["x"].max()), abs(self._pixelmaps["x"].min())))
            + 2
        )
        self._visual_img_shape: Tuple[int, int] = (y_minimum, x_minimum)
        self._img_center_x: int = int(self._visual_img_shape[1] / 2)
        self._img_center_y: int = int(self._visual_img_shape[0] / 2)
        self._visual_pixelmap_x: numpy.typing.NDArray[numpy.int32] = cast(
            numpy.typing.NDArray[numpy.int32],
            numpy.array(self._pixelmaps["x"], dtype=numpy.int32)
            + self._visual_img_shape[1] // 2
            - 1,
        ).flatten()
        self._visual_pixelmap_y: numpy.typing.NDArray[numpy.int32] = cast(
            numpy.typing.NDArray[numpy.int32],
            numpy.array(self._pixelmaps["y"], dtype=numpy.int32)
            + self._visual_img_shape[0] // 2
            - 1,
        ).flatten()

    def _update_resolution_rings_status(self) -> None:
        new_state = self._resolution_rings_check_box.isChecked()
        if self._resolution_rings_enabled is True and new_state is False:
            text_item: Any
            for text_item in self._resolution_rings_textitems:
                self._image_view.scene.removeItem(text_item)
            self._resolution_rings_canvas.setData([], [])
            self._resolution_rings_enabled = False
        if self._resolution_rings_enabled is False and new_state is True:
            for text_item in self._resolution_rings_textitems:
                self._image_view.getView().addItem(text_item)
            self._resolution_rings_enabled = True
            self._draw_resolution_rings()

    def _update_resolution_rings_radii(self) -> None:
        was_enabled: bool = self._resolution_rings_check_box.isChecked()
        self._resolution_rings_check_box.setChecked(False)

        items: List[str] = str(self._resolution_rings_lineedit.text()).split(",")
        if items:
            item: str
            self._resolution_rings_in_a = [
                float(item) for item in items if item != "" and float(item) != 0.0
            ]
        else:
            self._resolution_rings_in_a = []

        x: float
        self._resolution_rings_textitems = [
            pyqtgraph.TextItem(text="{0}A".format(x), anchor=(0.5, 0.8))
            for x in self._resolution_rings_in_a
        ]

        if was_enabled is True:
            self._resolution_rings_check_box.setChecked(True)

        self._draw_resolution_rings()

    def _draw_resolution_rings(self) -> None:
        # Draws the resolution rings.

        if self._resolution_rings_enabled is False:
            return

        try:
            filename: str = self._events[self._current_event]["filename"]
            indx: int = self._events[self._current_event]["event"]
            if self._clen_from:
                if indx == -1:
                    detector_distance: float = self._input_files[filename][
                        self._clen_from
                    ]
                else:
                    detector_distance = self._input_files[filename][self._clen_from][
                        indx
                    ]
            else:
                detector_distance = self._clen * 1e3
            if self._photon_energy_from:
                if indx == -1:
                    photon_energy: float = self._input_files[filename][
                        self._photon_energy_from
                    ]
                else:
                    photon_energy = self._input_files[filename][
                        self._photon_energy_from
                    ][indx]
            else:
                photon_energy = self._photon_energy
            lambda_: float = constants.h * constants.c / (photon_energy * constants.e)
            resolution_rings_in_pix: List[float] = [1.0]
            resolution_rings_in_pix.extend(
                [
                    2.0
                    * self._pixel_size
                    * (detector_distance * 1e-3 + self._coffset)
                    * numpy.tan(
                        2.0 * numpy.arcsin(lambda_ / (2.0 * resolution * 1e-10))
                    )
                    for resolution in self._resolution_rings_in_a
                ]
            )
        except (TypeError, KeyError):
            print(
                "Beam energy or detector distance information is not available. "
                "Resolution rings cannot be drawn."
            )
            self._resolution_rings_check_box.setChecked(False)
        else:
            self._resolution_rings_canvas.setData(
                [self._img_center_x] * len(resolution_rings_in_pix),
                [self._img_center_y] * len(resolution_rings_in_pix),
                symbol="o",
                size=resolution_rings_in_pix,
                pen=self._resolution_rings_pen,
                brush=(0, 0, 0, 0),
                pxMode=False,
            )

            index: int
            item: Any
            for index, item in enumerate(self._resolution_rings_textitems):
                item.setPos(
                    (self._img_center_x + resolution_rings_in_pix[index + 1] / 2.0),
                    self._img_center_y,
                )

    def _hist_range_changed(self) -> None:
        self._levels_range = self._image_hist.getLevels()
        self._ui.min_range_le.setText(f"{int(self._levels_range[0])}")
        self._ui.max_range_le.setText(f"{int(self._levels_range[1])}")

    def _change_levels(self) -> None:
        self._levels_range = (
            float(self._ui.min_range_le.text()),
            float(self._ui.max_range_le.text()),
        )
        if self._levels_range[1] < self._levels_range[0]:
            self._levels_range = (
                float(self._ui.min_range_le.text()),
                float(self._ui.min_range_le.text()),
            )
            self._ui.max_range_le.setText(self._ui.min_range_le.text())
        self._ui.auto_range_cb.setChecked(False)
        self._update_image_and_peaks()

    def _mouse_moved(self, pos: Any) -> None:
        data: numpy.typing.NDArray[Any] = self._image_view.image
        scene_pos: Any = self._image_view.getImageItem().mapFromScene(pos)
        row: int = int(scene_pos.x())
        col: int = int(scene_pos.y())
        if (0 <= row < data.shape[0]) and (0 <= col < data.shape[1]):
            value: Any = data[row, col]
        else:
            value = numpy.nan
        self._ui.intensity_label.setText(f"Intensity = {value:.2f}")

    def _next_pattern(self) -> None:
        if self._current_event < self._num_events - 1:
            self._current_event += 1
        else:
            self._current_event = 0
        self._update_image_and_peaks()

    def _previous_pattern(self) -> None:
        if self._current_event > 0:
            self._current_event -= 1
        else:
            self._current_event = self._num_events - 1
        self._update_image_and_peaks()

    def _random_pattern(self) -> None:
        self._current_event = randrange(self._num_events)
        self._update_image_and_peaks()

    def _play(self) -> None:
        self._refresh_timer.start(1000)
        self._ui.pause_button.setEnabled(True)
        self._ui.play_button.setEnabled(False)

    def _shuffle_changed(self) -> None:
        self._refresh_timer.timeout.disconnect()
        if self._ui.shuffle_button.isChecked():
            self._refresh_timer.timeout.connect(self._random_pattern)
        else:
            self._refresh_timer.timeout.connect(self._next_pattern)

    def _pause(self) -> None:
        self._refresh_timer.stop()
        self._ui.pause_button.setEnabled(False)
        self._ui.play_button.setEnabled(True)

    def _update_image_and_peaks(self) -> None:
        # Updates the image and Bragg peaks shown by the viewer.
        filename: str = self._events[self._current_event]["filename"]
        indx: int = self._events[self._current_event]["event"]
        if indx == -1:
            data: numpy.typing.NDArray[Any] = self._input_files[filename][
                self._hdf5_data_path
            ][()]
        else:
            data = self._input_files[filename][self._hdf5_data_path][indx]

        self._frame_data_img[
            self._visual_pixelmap_y, self._visual_pixelmap_x
        ] = data.ravel().astype(self._frame_data_img.dtype)

        if self._ui.auto_range_cb.isChecked():
            values = data.flatten()
            values.sort()
            nvalues: int = len(values)
            self._levels_range = (
                values[nvalues // 100],
                values[nvalues - nvalues // 100],
            )

        self._image_view.setImage(
            self._frame_data_img.T,
            autoLevels=False,
            levels=self._levels_range,
            autoRange=False,
            autoHistogramRange=False,
        )

        self._update_peaks()
        self.statusBar().showMessage("Showing {0}, event {1}".format(filename, indx))

    def _update_mask_image(self) -> None:
        if self._ui.show_mask_cb.isChecked():
            self._mask_image.setImage(
                self._mask, compositionMode=QtGui.QPainter.CompositionMode_SourceOver
            )
        else:
            self._mask_image.clear()

    def _update_peaks(self) -> None:
        # Updates the Bragg peaks shown by the viewer.
        peak_list_y_in_frame: List[float] = []
        peak_list_x_in_frame: List[float] = []
        if self._ui.show_peaks_cb.isChecked():
            filename: str = self._events[self._current_event]["filename"]
            indx: int = self._events[self._current_event]["event"]
            if self._hdf5_peaks_path not in self._input_files[filename]:
                print(f"Peaks dataset {self._hdf5_peaks_path} not found in {filename}")
                return
            num_peaks: int = self._input_files[filename][self._hdf5_peaks_path][
                "nPeaks"
            ][indx]
            peak_fs: float
            peak_ss: float
            for peak_fs, peak_ss in zip(
                self._input_files[filename][self._hdf5_peaks_path]["peakXPosRaw"][indx][
                    :num_peaks
                ],
                self._input_files[filename][self._hdf5_peaks_path]["peakYPosRaw"][indx][
                    :num_peaks
                ],
            ):
                peak_index_in_slab: int = int(round(peak_ss)) * self._data_shape[
                    1
                ] + int(round(peak_fs))
                y_in_frame: float = self._visual_pixelmap_y[peak_index_in_slab]
                x_in_frame: float = self._visual_pixelmap_x[peak_index_in_slab]
                peak_list_x_in_frame.append(y_in_frame)
                peak_list_y_in_frame.append(x_in_frame)
        self._peak_canvas.setData(
            x=peak_list_y_in_frame,
            y=peak_list_x_in_frame,
            symbol="o",
            brush=(255, 255, 255, 0),
            size=[5] * len(peak_list_x_in_frame),
            pen=self._ring_pen,
            pxMode=False,
        )


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument(  # type: ignore
    "input_files",
    nargs=-1,
    type=click.Path(exists=True),
)
@click.option(  # type: ignore
    "--geometry",
    "-g",
    "geometry_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=True,
)
@click.option(  # type: ignore
    "--hdf5-data-path",
    "-d",
    "hdf5_data_path",
    nargs=1,
    type=str,
    required=False,
    default="/data/data",
)
@click.option(  # type: ignore
    "--hdf5-peaks-path",
    "-p",
    "hdf5_peaks_path",
    nargs=1,
    type=str,
    required=False,
)
@click.option(  # type: ignore
    "--mask",
    "-m",
    "mask_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
)
@click.option(  # type: ignore
    "--mask-hdf5-path",
    "mask_hdf5_path",
    nargs=1,
    type=str,
    required=False,
)
def main(
    input_files,
    hdf5_data_path,
    geometry_filename,
    hdf5_peaks_path,
    mask_filename,
    mask_hdf5_path,
) -> None:
    """ """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = Viewer(
        input_files,
        hdf5_data_path,
        geometry_filename,
        hdf5_peaks_path,
        mask_filename,
        mask_hdf5_path,
    )
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

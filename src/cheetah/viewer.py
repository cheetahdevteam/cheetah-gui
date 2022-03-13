"""
Cheetah Viewer.

This module contains Cheetah image viewer.
"""
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
from typing import Any, List, Dict, Union, Tuple, cast

from cheetah import __file__ as cheetah_src_path
from cheetah.frame_retrieval.base import CheetahFrameRetrieval, TypeEventData
from cheetah.frame_retrieval.frame_retrieval_files import H5FilesRetrieval
from cheetah.frame_retrieval.frame_retrieval_om import OmRetrieval


class Viewer(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        frame_retrieval: CheetahFrameRetrieval,
        geometry_filename: str,
        mask_filename: Union[str, None] = None,
        mask_hdf5_path: str = "/data/data",
    ) -> None:
        """
        Cheetah Viewer.

        This class implements Cheetah frame viewer. The viewer displays data frames
        retrieved by Cheetah Frame Retrieval applying detector geometry from privided
        [CrystFEL geometry file][https://www.desy.de/~twhite/crystfel/manual-crystfel_geometry.html].
        It can also optionally display positions of the detected peaks and the mask
        overlaid over the image.

        Arguments:

            frame_retrieval: An instance of
            [CheetaFrameRetrieval][cheetah.frame_retrieval.base.CheetahFrameRetrieval]
            class.

            geometry_filename: The path of the geometry file.

            mask_filename: The path of the mask file. If the value of this parameter
                is None and the mask file is not specified in the geometry file, the
                option of showing mask will be disabled. Defaults to None.

            mask_hdf5_path: The path to the mask dataset in the mask HDF5 file.
                Defaults to '/data/data'.
        """
        super(Viewer, self).__init__()
        self._ui: Any = uic.loadUi(
            (pathlib.Path(cheetah_src_path) / "../ui_src/viewer.ui").resolve(), self
        )
        self.setWindowTitle(f"Cheetah Viewer")
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self.show()

        self._frame_retrieval: CheetahFrameRetrieval = frame_retrieval

        self._load_geometry(geometry_filename)
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

        self._events: List[str] = self._frame_retrieval.get_event_list()
        self._num_events: int = len(self._events)
        self._current_event_index: int = 0
        self._retrieve_current_data()

        self._ui.show_peaks_cb.setEnabled(False)

        self._data_shape: Tuple[int, int] = (
            self._current_event_data["data"].shape[-2],
            self._current_event_data["data"].shape[-1],
        )
        self._frame_data_img: numpy.typing.NDArray[Any] = numpy.zeros(
            self._visual_img_shape, dtype=self._current_event_data["data"].dtype
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
        # Loads CrystFEL goemetry using om.utils module.
        self._geometry: crystfel_geometry.TypeDetector
        beam: crystfel_geometry.TypeBeam
        self._geometry, beam, __ = crystfel_geometry.load_crystfel_geometry(
            filename=geometry_filename
        )
        self._pixelmaps: crystfel_geometry.TypePixelMaps = (
            crystfel_geometry.compute_pix_maps(geometry=self._geometry)
        )

        first_panel: str = list(self._geometry["panels"].keys())[0]
        self._pixel_size: float = self._geometry["panels"][first_panel]["res"]
        self._clen_from: str = self._geometry["panels"][first_panel]["clen_from"]
        if self._clen_from == "":
            self._clen: float = self._geometry["panels"][first_panel]["clen"]
        self._coffset: float = self._geometry["panels"][first_panel]["coffset"]
        self._photon_energy_from: str = beam["photon_energy_from"]
        if self._photon_energy_from == "":
            self._photon_energy: float = beam["photon_energy"]
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
            if self._clen_from:
                detector_distance: float = self._current_event_data["clen"] * 1e3
            else:
                detector_distance = self._clen * 1e3
            if self._photon_energy_from:
                photon_energy: float = self._current_event_data["photon_energy"]
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

    def _retrieve_current_data(self) -> None:
        self._current_event_data: TypeEventData = self._frame_retrieval.get_data(
            self._current_event_index
        )

    def _next_pattern(self) -> None:
        if self._current_event_index < self._num_events - 1:
            self._current_event_index += 1
        else:
            self._current_event_index = 0
        self._retrieve_current_data()
        self._update_image_and_peaks()

    def _previous_pattern(self) -> None:
        if self._current_event_index > 0:
            self._current_event_index -= 1
        else:
            self._current_event_index = self._num_events - 1
        self._retrieve_current_data()
        self._update_image_and_peaks()

    def _random_pattern(self) -> None:
        self._current_event_index = randrange(self._num_events)
        self._retrieve_current_data()
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
        data: numpy.typing.NDArray[Any] = self._current_event_data["data"]

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
        self.statusBar().showMessage(f"{self._events[self._current_event_index]}")

    def _update_mask_image(self) -> None:
        if self._ui.show_mask_cb.isChecked():
            self._mask_image.setImage(
                self._mask, compositionMode=QtGui.QPainter.CompositionMode_SourceOver
            )
        else:
            self._mask_image.clear()

    def _update_peaks(self) -> None:
        # Updates the Bragg peaks shown by the viewer.
        if "peaks" not in self._current_event_data.keys():
            self._ui.show_peaks_cb.setEnabled(False)
        else:
            self._ui.show_peaks_cb.setEnabled(True)
        peak_list_y_in_frame: List[float] = []
        peak_list_x_in_frame: List[float] = []
        if (
            self._ui.show_peaks_cb.isChecked()
            and "peaks" in self._current_event_data.keys()
        ):
            peak_fs: float
            peak_ss: float
            for peak_fs, peak_ss in zip(
                self._current_event_data["peaks"]["fs"],
                self._current_event_data["peaks"]["ss"],
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


def _get_hdf5_retrieval_parameters(geometry_filename: str) -> Dict[str, Any]:
    # This function is used internally to get parameters for hdf5 data retrieval from
    # the geometry file.
    geometry: crystfel_geometry.TypeDetector
    beam: crystfel_geometry.TypeBeam
    geometry, beam, __ = crystfel_geometry.load_crystfel_geometry(
        filename=geometry_filename
    )
    first_panel: str = list(geometry["panels"].keys())[0]
    return {
        "hdf5_data_path": geometry["panels"][first_panel]["data"],
        "clen_path": geometry["panels"][first_panel]["clen_from"],
        "photon_energy_path": beam["photon_energy_from"],
    }


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument("input_files", nargs=-1, type=click.Path(exists=True), required=True, metavar="INPUT_FILE(S)")  # type: ignore
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
    "--mask",
    "-m",
    "mask_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
    help="mask HDF5 file, default: None",
)
@click.option(  # type: ignore
    "--mask-hdf5-path",
    "hdf5_mask_path",
    nargs=1,
    type=str,
    required=False,
    default="/data/data",
    help="path to the mask dataset in the mask HDF5 file, default: /data/data",
)
@click.option(  # type: ignore
    "--hdf5-data-path",
    "-d",
    "hdf5_data_path",
    nargs=1,
    type=str,
    required=False,
    help="path to the image dataset in the input HDF5 files, default: HDF5 data path "
    "specified in the geometry file",
)
@click.option(  # type: ignore
    "--hdf5-peaks-path",
    "-p",
    "hdf5_peaks_path",
    nargs=1,
    type=str,
    required=False,
    help="path to the peaks dataset in the input HDF5 files, default: None",
)
@click.option(  # type: ignore
    "--om-events", "-o", "om_events", is_flag=True, help="retrieve frame data from OM"
)
@click.option(  # type: ignore
    "--om-source",
    "-s",
    "om_source",
    nargs=1,
    type=str,
    required=False,
    default="",
    help="OM source string, default: ''",
)
@click.option(  # type: ignore
    "--om-config",
    "-c",
    "om_config",
    nargs=1,
    type=click.Path(),
    required=False,
    default="monitor.yaml",
    help="OM configuration file, default: monitor.yaml",
)
@click.option(  # type: ignore
    "--om-peaks-file",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
    help="peak list file written by Cheetah processing layer in OM, default: None",
)
def main(
    input_files: List[str],
    geometry_filename: str,
    mask_filename: Union[str, None],
    hdf5_mask_path: str,
    hdf5_data_path: Union[str, None],
    hdf5_peaks_path: Union[str, None],
    om_events: bool,
    om_source: str,
    om_config: str,
    om_peaks_file: Union[str, None],
) -> None:
    """
    Cheetah Viewer. The viewer displays images from various sources applying detector
    geometry from the provided geometry file in CrystFEL format. The viewer can
    optionally display positions of the detected peaks when they are available. It can
    also show mask if the mask file is provided either as a command line argument or as
    an entry in the geometry file.

    The viewer can retrieve images from the following sources:

    1) HDF5 files. To display images stored in single- or multi-event HDF5 files
    one or several .h5 or .cxi files must be provided as INPUT_FILE(S).
    Additionally, '--hdf5-data-path' / '-p' option may be used to specify where
    the images are stored in the HDF5 files. Otherwise, the path specified in
    the geometry file will be used. '--hdf5-peaks-path' / '-p' option may be
    used to specify where the peaks dataset is stored in the HDF5 files. If this
    option is not set, detected peak positions won't be shown.

    \b
    Usage example: cheetah_viewer.py data_*.h5 -g current.geom -d /data/data -p /data/peaks

    2) OM data retrieval layer. To display images retrieved by OM '--om-events' / '-o'
    flag must be set and one text file containing a list of OM event IDs must be
    provided as INPUT_FILE. Both OM source string ('--om-source / '-s') and OM config
    file ('--om-config' / '-c') must also be provided. Additionally, Cheetah peak list
    file ('--om-peaks-file') can be used to retrieve information about detected peaks.
    If peak list file is not specified, peaks will be detected using peakfinder8
    parameters from the provided config file.

    \b
    Usage example: cheetah_viewer.py hits.lst -g current.geom -s exp=mfxlz0420:run=141 -c monitor.yaml --om-peaks-file=peaks.txt
    """
    if om_events:
        print("Activating frame retrieval from OM data retrieval layer.")
        if not pathlib.Path(om_config).is_file():
            sys.exit(
                f"Error: Invalid value for '--om-config' / '-c': Path {om_config} "
                f"does not exist."
            )
        input_file: str = input_files[0]
        parameters: Dict[str, Any] = {
            "om_sources": {input_file: om_source},
            "om_configs": {input_file: om_config},
        }
        if om_peaks_file:
            parameters["peak_lists"] = {input_file: om_peaks_file}
        frame_retrieval: CheetahFrameRetrieval = OmRetrieval(
            [
                input_file,
            ],
            parameters,
        )

    if input_files[0].endswith(".h5") or input_files[0].endswith(".cxi"):
        print("Activating frame retrieval from HDF5 files.")
        parameters = _get_hdf5_retrieval_parameters(geometry_filename)
        if hdf5_data_path:
            parameters["hdf5_data_path"] = hdf5_data_path
        if hdf5_peaks_path:
            parameters["hdf5_peaks_path"] = hdf5_peaks_path
        frame_retrieval = H5FilesRetrieval(input_files, parameters)

    app: Any = QtWidgets.QApplication(sys.argv)
    _ = Viewer(
        frame_retrieval,
        geometry_filename,
        mask_filename,
        hdf5_mask_path,
    )
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

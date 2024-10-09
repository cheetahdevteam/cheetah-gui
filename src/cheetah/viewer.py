"""
Cheetah Viewer.

This module contains Cheetah image viewer.
"""

import logging
import logging.config
import pathlib
import sys
from random import randrange
from typing import Any, Callable, Dict, List, Optional, TextIO, Tuple, Union

import click  # type: ignore
import h5py  # type: ignore
import numpy
import numpy.typing
import pyqtgraph  # type: ignore
import ruamel.yaml  # type: ignore
import yaml
from numpy.typing import NDArray
from om.algorithms.crystallography import Peakfinder8PeakDetection
from om.lib.geometry import (
    Beam,
    DataVisualizer,
    Detector,
    DetectorLayoutInformation,
    PixelMaps,
    VisualizationPixelMaps,
    _compute_pix_maps,
    _read_crystfel_geometry_from_text,
    _retrieve_layout_info_from_geometry,
)
from PyQt5 import QtCore, QtGui, QtWidgets, uic  # type: ignore
from scipy import constants  # type: ignore
from scipy.ndimage.morphology import binary_dilation, binary_erosion  # type: ignore

from cheetah import __file__ as cheetah_src_path
from cheetah.frame_retrieval.base import CheetahFrameRetrieval, EventData, PeakList
from cheetah.frame_retrieval.frame_retrieval_files import H5FilesRetrieval
from cheetah.frame_retrieval.frame_retrieval_om import OmRetrieval
from cheetah.frame_retrieval.frame_retrieval_stream import StreamRetrieval
from cheetah.utils.logging import logging_config

logger = logging.getLogger("cheetah_viewer")


class Viewer(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        frame_retrieval: CheetahFrameRetrieval,
        geometry_lines: List[str],
        mask_filename: Optional[str] = None,
        mask_hdf5_path: str = "/data/data",
        open_tab: int = 0,
        pt_config_path: Optional[str] = None,
        geometry_path: Optional[str] = None,
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

            geometry_lines: A list of lines from the geometry file.

            mask_filename: The path of the mask file. If the value of this parameter
                is None and the mask file is not specified in the geometry file, the
                option of showing mask will be disabled. Defaults to None.

            mask_hdf5_path: The path to the mask dataset in the mask HDF5 file.
                Defaults to '/data/data'.

            open_tab: Which tab to show when the GUI opens (0 - Show, 1 - Maskmaker).
                Defaults to 0.

            pt_config_path: The path to the OM configuration file for peakfinder
                parameter tweaker. Defaults to None. Used only as a default path for
                the file dialog. If not provided, the current working directory is used.

            geometry_path: The path to the CrystFEL geometry file. Defaults to None.
                Used only as a default path for the file dialog. If not provided, the
                current working directory is used.
        """
        super(Viewer, self).__init__()
        self._ui: Any = uic.loadUi(
            (pathlib.Path(cheetah_src_path) / "../ui_src/viewer.ui").resolve(), self
        )
        self.setWindowTitle("Cheetah Viewer")
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self.show()

        self._ui.tab_widget.setCurrentIndex(open_tab)
        self._ui.tab_widget.currentChanged.connect(self._tab_changed)
        self._current_tab: int = self._ui.tab_widget.currentIndex()

        self._load_geometry(geometry_lines)
        if mask_filename:
            self._mask_filename: str = mask_filename
            if mask_hdf5_path:
                self._mask_hdf5_path: str = mask_hdf5_path
            else:
                self._mask_hdf5_path = "/data/data"

        if (
            self._mask_filename
            and self._mask_hdf5_path
            and pathlib.Path(self._mask_filename).is_file()
        ):
            mask_file: Any
            with h5py.File(self._mask_filename) as mask_file:
                if self._mask_hdf5_path not in mask_file:
                    logger.warning(
                        f"Dataset {self._mask_hdf5_path} not found in the mask file "
                        f"{self._mask_filename}."
                    )
                    self._ui.show_mask_cb.setEnabled(False)
                else:
                    self._mask: NDArray[Any] = self._create_mask_image(
                        mask_file[self._mask_hdf5_path][()]
                    )

        else:
            self._ui.show_mask_cb.setEnabled(False)

        self._pt_config_path: pathlib.Path = (
            pathlib.Path(pt_config_path) if pt_config_path else pathlib.Path.cwd()
        )
        self._geometry_path: pathlib.Path = (
            pathlib.Path(geometry_path) if geometry_path else pathlib.Path.cwd()
        )
        self._geometry_lines: List[str] = geometry_lines

        self._frame_retrieval: CheetahFrameRetrieval = frame_retrieval
        self._events: List[str] = self._frame_retrieval.get_event_list()
        self._num_events: int = len(self._events)
        if self._num_events == 0:
            logger.info("No images can be retrieved from the input sources.")
            sys.exit(0)

        self._current_event_index: int = 0
        self._retrieve_current_data()

        self._ui.total_number_label.setText(f"/{self._num_events}")
        self._index_regex: Any = QtCore.QRegExp(r"[1-9]\d*")
        self._index_validator: Any = QtGui.QRegExpValidator()
        self._index_validator.setRegExp(self._index_regex)
        self._ui.current_event_index_le.setValidator(self._index_validator)
        self._ui.current_event_index_le.editingFinished.connect(self._go_to_pattern)

        self._show_pixel_values: bool = False
        self._pixel_value_labels: Dict[Tuple[int, int], Any] = {}

        self._ui.show_peaks_cb.setEnabled(False)

        self._empty_frame: NDArray[Any] = numpy.empty(self._data_shape)
        self._empty_frame[:] = numpy.nan

        self._frame_data_img: NDArray[Any] = numpy.zeros(self._visual_img_shape)

        pyqtgraph.setConfigOption("background", 0.2)

        self._image_widget: Any = self._ui.image_view
        self._image_view: Any = self._image_widget.getView()
        self._image_item: Any = self._image_widget.getImageItem()
        self._image_widget.ui.menuBtn.hide()
        self._image_widget.ui.roiBtn.hide()

        self._image_widget.scene.sigMouseMoved.connect(self._mouse_moved)
        self._image_view.sigRangeChanged.connect(self._visible_image_range_changed)
        self._image_view.invertY(False)

        self._image_hist = self._image_widget.getHistogramWidget()
        self._image_hist.sigLevelsChanged.connect(self._hist_range_changed)
        self._levels_range: Tuple[Union[int, float], Union[int, float]] = (0, 1)
        self._ui.auto_range_cb.setChecked(True)
        self._ui.auto_range_cb.stateChanged.connect(self._update_image)

        self._float_regex: Any = QtCore.QRegExp(r"-?\d*\.?\d*([eE][+-]?\d+)?")
        self._float_validator: Any = QtGui.QRegExpValidator()
        self._float_validator.setRegExp(self._float_regex)
        self._ui.min_range_le.setValidator(self._float_validator)
        self._ui.max_range_le.setValidator(self._float_validator)

        self._ui.min_range_le.editingFinished.connect(self._change_levels)
        self._ui.max_range_le.editingFinished.connect(self._change_levels)

        self._ui.next_button.clicked.connect(self._next_pattern)
        self._ui.previous_button.clicked.connect(self._previous_pattern)
        self._ui.random_button.clicked.connect(self._random_pattern)
        self._ui.play_button.clicked.connect(self._play)
        self._ui.pause_button.clicked.connect(self._pause)
        self._ui.shuffle_button.setCheckable(True)
        self._ui.shuffle_button.clicked.connect(self._shuffle_changed)

        self._init_show_tab()
        self._init_maskmaker_tab()
        self._init_tweaker_tab()
        self._init_geometry_tab()

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._next_pattern)
        self._ui.pause_button.setEnabled(False)

        self._update_image_and_peaks()
        self._tab_changed()

    def _init_show_tab(self) -> None:
        # Initialize UI elements in the Show tab
        self._ring_pen: Any = pyqtgraph.mkPen("r", width=2)
        self._peak_canvas: Any = pyqtgraph.ScatterPlotItem()
        self._image_view.addItem(self._peak_canvas)

        self._refl_canvas: Any = pyqtgraph.ScatterPlotItem()
        self._image_view.addItem(self._refl_canvas)

        self._resolution_rings_in_a: List[float] = [10.0, 6.0, 4.0, 3.0, 2.0, 1.5]
        self._resolution_rings_textitems: List[Any] = [
            pyqtgraph.TextItem(
                text="{0}A".format(x), anchor=(0.5, 0.8), color=(0, 255, 0)
            )
            for x in self._resolution_rings_in_a
        ]
        self._resolution_rings_enabled: bool = False
        self._resolution_rings_pen: Any = pyqtgraph.mkPen("g", width=1)
        self._resolution_rings_canvas: Any = pyqtgraph.ScatterPlotItem()
        self._image_view.addItem(self._resolution_rings_canvas)

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
        self._resolution_rings_check_box.clicked.connect(
            self._update_resolution_rings_status
        )

        self._mask_image: Any = pyqtgraph.ImageItem()
        self._mask_image.setZValue(1)
        self._mask_image.setOpacity(0.5)
        self._image_view.addItem(self._mask_image)
        self._ui.show_mask_cb.stateChanged.connect(
            lambda state: self._update_mask_image()
        )
        self._ui.show_peaks_cb.stateChanged.connect(self._update_peaks)

        self._ui.next_crystal_button.clicked.connect(self._next_crystal)
        self._ui.previous_crystal_button.clicked.connect(self._previous_crystal)
        self._ui.next_crystal_button.setEnabled(False)
        self._ui.previous_crystal_button.setEnabled(False)
        self._ui.show_no_crystals_rb.clicked.connect(self._update_reflections)
        self._ui.show_one_crystal_rb.clicked.connect(self._update_reflections)
        self._ui.show_all_crystals_rb.clicked.connect(self._update_reflections)
        self._crystal_to_show: int = 0

    def _init_maskmaker_tab(self) -> None:
        # Initialize UI elements in the Maskmaker tab
        self._ui.mask_rb.toggled.connect(self._change_mask_mode)
        self._ui.unmask_rb.toggled.connect(self._change_mask_mode)
        self._ui.toggle_rb.toggled.connect(self._change_mask_mode)
        self._change_mask_mode()

        self._ui.rectangular_roi_button.clicked.connect(self._mask_rectangular_roi)
        self._ui.circular_roi_button.clicked.connect(self._mask_curcular_roi)
        self._ui.outside_histogram_button.clicked.connect(self._mask_outside_histogram)
        self._ui.panel_edges_button.clicked.connect(self._mask_panel_edges)
        self._ui.dilate_mask_button.clicked.connect(self._dilate_mask)
        self._ui.erode_mask_button.clicked.connect(self._erode_mask)
        self._ui.add_mask_from_file_button.clicked.connect(self._add_mask_from_file)
        self._ui.clear_mask_button.clicked.connect(self._clear_mask)

        self._ui.brush_button.clicked.connect(self._start_brush)
        self._ui.brush_button.setCheckable(True)
        self._ui.brush_size_sb.valueChanged.connect(self._change_brush_size)
        self._ui.add_brush_button.clicked.connect(self._add_brush)
        self._ui.discard_brush_button.clicked.connect(self._discard_brush)

        self._ui.save_mask_button.clicked.connect(self._save_mask)

        self._rectangular_roi: Any = pyqtgraph.RectROI([0, 0], [100, 100])
        self._circular_roi: Any = pyqtgraph.CircleROI([0, 110], [100, 100])
        self._rectangular_roi.setZValue(10)
        self._circular_roi.setZValue(10)
        self._image_view.addItem(self._rectangular_roi)
        self._image_view.addItem(self._circular_roi)

        self._brush_image: Any = None

        self._maskmaker_image: Any = pyqtgraph.ImageItem()
        self._maskmaker_image.setZValue(2)
        self._maskmaker_image.setOpacity(0.5)
        self._image_view.addItem(self._maskmaker_image)
        self._maskmaker_mask: NDArray[numpy.int_] = numpy.zeros(
            self._data_shape, dtype=int
        )
        self._maskmaker_visual_mask: NDArray[numpy.int_] = numpy.zeros(
            self._visual_img_shape, dtype=int
        ).T
        self._update_maskmaker_image()

        self._image_widget.scene.sigMouseClicked.connect(self._mouse_clicked)

    def _init_tweaker_tab(self) -> None:
        # Set validators for peak finder parameters
        self._int_regex: Any = QtCore.QRegExp(r"[0-9]*")
        self._int_validator: Any = QtGui.QRegExpValidator()
        self._int_validator.setRegExp(self._int_regex)

        self._ui.pt_adc_thresh_le.setValidator(self._float_validator)
        self._ui.pt_minimum_snr_le.setValidator(self._float_validator)
        self._ui.pt_min_res_le.setValidator(self._int_validator)
        self._ui.pt_max_res_le.setValidator(self._int_validator)
        self._ui.pt_min_pixel_count_le.setValidator(self._int_validator)
        self._ui.pt_max_pixel_count_le.setValidator(self._int_validator)
        self._ui.pt_local_bg_radius_le.setValidator(self._int_validator)
        self._ui.pt_min_peaks_le.setValidator(self._int_validator)

        # Disable peak finder parameter inputs
        self._pt_inputs: List[Any] = [
            self._ui.pt_adc_thresh_le,
            self._ui.pt_minimum_snr_le,
            self._ui.pt_min_res_le,
            self._ui.pt_max_res_le,
            self._ui.pt_min_pixel_count_le,
            self._ui.pt_max_pixel_count_le,
            self._ui.pt_local_bg_radius_le,
            self._ui.pt_min_peaks_le,
        ]

        for input_widget in self._pt_inputs:
            input_widget.setEnabled(False)
        self._ui.pt_load_mask_button.setEnabled(False)
        self._ui.save_config_button.setEnabled(False)

        # Connect parameter edits
        for input_widget in self._pt_inputs:
            input_widget.editingFinished.connect(
                lambda w=input_widget: self._update_peakfinder_parameters(w)
            )
        self._ui.pt_load_mask_button.clicked.connect(self._load_mask_for_peakfinder)
        self._ui.load_config_button.clicked.connect(self._load_peakfinder_parameters)
        self._ui.save_config_button.clicked.connect(self._save_peakfinder_parameters)

        self._peakfinder: Optional[Peakfinder8PeakDetection] = None
        self._pt_mask: Optional[NDArray[numpy.int_]] = None
        self._pt_num_peaks: int = 0

    def _init_geometry_tab(self) -> None:
        # Initialize UI elements in the Geometry tab
        self._geometry_rois: List[Any] = []
        self._geometry_roi_radii: List[float] = [300.0, 100.0, 50.0]
        i: int
        for i in range(len(self._geometry_roi_radii)):
            radius: float = self._geometry_roi_radii[i]
            roi: Any = pyqtgraph.CircleROI(
                [self._img_center_x - radius, self._img_center_y - radius],
                [2 * radius, 2 * radius],
                pen=(4, 9),
                movable=True if i == 0 else False,
            )
            self._image_view.addItem(roi)
            self._geometry_rois.append(roi)

        self._geometry_rois[0].sigRegionChanged.connect(self._move_geometry_rois)
        for i in range(1, len(self._geometry_rois)):
            self._geometry_rois[i].sigRegionChangeFinished.connect(
                self._update_geometry_labels
            )

        self._geometry_shift: Tuple[float, float] = (0.0, 0.0)
        self._ui.save_geometry_button.clicked.connect(self._save_geometry)

    def _load_geometry(self, geometry_lines: List[str]) -> None:
        # Loads CrystFEL goemetry using om.lib.geometry module.
        self._geometry: Detector
        beam: Beam
        self._geometry, beam, _ = _read_crystfel_geometry_from_text(
            text_lines=geometry_lines
        )
        first_panel: str = list(self._geometry.panels.keys())[0]

        # Pixel size (in 1/m)
        self._pixel_size: float = self._geometry.panels[first_panel].res
        # Detector distance
        self._clen_from: str = self._geometry.panels[first_panel].clen_from
        if self._clen_from == "":
            self._clen: float = self._geometry.panels[first_panel].clen
        self._coffset: float = self._geometry.panels[first_panel].coffset
        # Photon energy
        self._photon_energy_from: str = beam.photon_energy_from
        if self._photon_energy_from == "":
            self._photon_energy: float = beam.photon_energy
        # Mask file
        self._mask_filename = self._geometry.panels[first_panel].mask_file
        self._mask_hdf5_path = self._geometry.panels[first_panel].mask

        pixel_maps: PixelMaps = _compute_pix_maps(geometry=self._geometry)
        self._radius_pixel_map: NDArray[numpy.float_] = pixel_maps.radius
        self._detector_layout_info: DetectorLayoutInformation = (
            _retrieve_layout_info_from_geometry(geometry=self._geometry)
        )

        self._data_visualizer: DataVisualizer = DataVisualizer(pixel_maps=pixel_maps)

        self._data_shape: Tuple[int, ...] = pixel_maps.x.shape
        self._visual_img_shape: Tuple[int, int] = (
            self._data_visualizer.get_min_array_shape_for_visualization()
        )
        self._img_center_x: int = int(self._visual_img_shape[1] / 2)
        self._img_center_y: int = int(self._visual_img_shape[0] / 2)

        self._visualization_pixel_maps: VisualizationPixelMaps = (
            self._data_visualizer.get_visualization_pixel_maps()
        )
        self._flattened_visualization_pixel_map_y = (
            self._visualization_pixel_maps.y.flatten()
        )
        self._flattened_visualization_pixel_map_x = (
            self._visualization_pixel_maps.x.flatten()
        )

    def _create_mask_image(self, mask_data: NDArray[Any]) -> NDArray[Any]:
        # Creates a mask image from the mask data.
        mask_data = 1 - mask_data
        mask_img: NDArray[Any] = numpy.zeros(
            self._visual_img_shape, dtype=mask_data.dtype
        )
        self._data_visualizer.visualize_data(
            data=mask_data, array_for_visualization=mask_img
        )
        mask: NDArray[Any] = numpy.zeros(shape=mask_img.T.shape + (4,))
        mask[:, :, 2] = mask_img.T
        mask[:, :, 3] = mask_img.T
        return mask

    def _tab_changed(self) -> None:
        if self._current_tab == 1:
            # Enable play button when turning off maskmaker
            self._ui.play_button.setEnabled(True)

        self._current_tab = self._ui.tab_widget.currentIndex()
        if self._current_tab == 1:
            # Disable refresh and play button when turning on maskmaker
            self._refresh_timer.stop()
            self._ui.pause_button.setEnabled(False)
            self._ui.play_button.setEnabled(False)

            # Hide resolution rings, peaks, reflections and input mask
            self._resolution_rings_canvas.setVisible(False)
            for text_item in self._resolution_rings_textitems:
                text_item.setVisible(False)
            self._peak_canvas.setVisible(False)
            self._refl_canvas.setVisible(False)
            self._mask_image.setVisible(False)

            # Hide geometry ROIs
            for roi in self._geometry_rois:
                roi.setVisible(False)

            # Show maskmaker mask and ROIs
            self._rectangular_roi.setVisible(True)
            self._circular_roi.setVisible(True)
            self._maskmaker_image.setVisible(True)

        elif self._current_tab == 2:
            # Hide reflections
            self._refl_canvas.setVisible(False)

            # Show resolution rings, peaks and input mask
            self._resolution_rings_canvas.setVisible(True)
            for text_item in self._resolution_rings_textitems:
                text_item.setVisible(True)
            self._peak_canvas.setVisible(True)
            self._mask_image.setVisible(True)

            # Hide maskmaker items
            self._rectangular_roi.setVisible(False)
            self._circular_roi.setVisible(False)
            self._maskmaker_image.setVisible(False)

            # Hide geometry ROIs
            for roi in self._geometry_rois:
                roi.setVisible(False)

            self._update_mask_image(self._pt_mask)
            self._update_peaks()

        elif self._current_tab == 3:
            # Show geometry ROIs
            for roi in self._geometry_rois:
                roi.setVisible(True)

            # Hide maskmaker items
            self._rectangular_roi.setVisible(False)
            self._circular_roi.setVisible(False)
            self._maskmaker_image.setVisible(False)

            self._update_geometry_labels()

        elif self._current_tab == 0:
            # Show resolution rings, peaks, reflections and input mask
            self._resolution_rings_canvas.setVisible(True)
            for text_item in self._resolution_rings_textitems:
                text_item.setVisible(True)
            self._peak_canvas.setVisible(True)
            self._refl_canvas.setVisible(True)
            self._mask_image.setVisible(True)

            # Hide maskmaker items
            self._rectangular_roi.setVisible(False)
            self._circular_roi.setVisible(False)
            self._maskmaker_image.setVisible(False)

            # Hide geometry ROIs
            for roi in self._geometry_rois:
                roi.setVisible(False)

            self._update_mask_image()
            self._update_peaks()

    def _update_resolution_rings_status(self) -> None:
        new_state = self._resolution_rings_check_box.isChecked()
        if self._resolution_rings_enabled is True and new_state is False:
            text_item: Any
            for text_item in self._resolution_rings_textitems:
                self._image_view.removeItem(text_item)
            self._resolution_rings_canvas.setData([], [])
            self._resolution_rings_enabled = False
        if self._resolution_rings_enabled is False and new_state is True:
            for text_item in self._resolution_rings_textitems:
                self._image_view.addItem(text_item)
            self._resolution_rings_enabled = True
            self._draw_resolution_rings()

    def _update_resolution_rings_radii(self) -> None:
        was_enabled: bool = self._resolution_rings_check_box.isChecked()
        self._resolution_rings_check_box.setChecked(False)
        self._update_resolution_rings_status()

        items: List[str] = str(self._resolution_rings_lineedit.text()).split(",")
        if items:
            self._resolution_rings_in_a = [
                float(item) for item in items if item != "" and float(item) != 0.0
            ]
        else:
            self._resolution_rings_in_a = []

        self._resolution_rings_textitems = [
            pyqtgraph.TextItem(
                text="{0}A".format(x), anchor=(0.5, 0.8), color=(0, 255, 0)
            )
            for x in self._resolution_rings_in_a
        ]

        if was_enabled is True:
            self._resolution_rings_check_box.setChecked(True)
        self._update_resolution_rings_status()
        self._draw_resolution_rings()

    def _draw_resolution_rings(self) -> None:
        # Draws the resolution rings.
        if self._resolution_rings_enabled is False:
            return

        try:
            if self._clen_from:
                if self._current_event_data.clen is not None:
                    detector_distance: float = self._current_event_data.clen * 1e3
                else:
                    raise ValueError
            else:
                detector_distance = self._clen * 1e3
            if self._photon_energy_from:
                if self._current_event_data.photon_energy is not None:
                    photon_energy: float = self._current_event_data.photon_energy
                else:
                    raise ValueError
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
        except (TypeError, KeyError, ValueError):
            logger.warning(
                "Beam energy or detector distance information is not available. "
                "Resolution rings cannot be drawn."
            )
            self._resolution_rings_check_box.setChecked(False)
            self._update_resolution_rings_status()
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
        try:
            self._ui.min_range_le.setText(f"{self._levels_range[0]:.7g}")
            self._ui.max_range_le.setText(f"{self._levels_range[1]:.7g}")
        except ValueError:
            pass

    def _change_levels(self) -> None:
        try:
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
            self._update_image()
        except ValueError:
            pass

    def _mouse_moved(self, pos: Any) -> None:
        data: NDArray[Any] = self._image_widget.image
        scene_pos: Any = self._image_item.mapFromScene(pos)
        row: int = int(scene_pos.x())
        col: int = int(scene_pos.y())
        if (0 <= row < data.shape[0]) and (0 <= col < data.shape[1]):
            value: Any = data[row, col]
        else:
            value = numpy.nan
        self._ui.intensity_label.setText(f"Intensity = {value:.4g}")

    def _retrieve_current_data(self) -> None:
        self._current_event_data: EventData = self._frame_retrieval.get_data(
            self._current_event_index
        )

    def _go_to_pattern(self) -> None:
        requested_event_index: int = int(self._ui.current_event_index_le.text()) - 1
        if requested_event_index > self._num_events - 1:
            requested_event_index = self._num_events - 1
        self._current_event_index = requested_event_index
        self._retrieve_current_data()
        self._update_image_and_peaks()

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

    def _update_image(self) -> None:
        if self._current_event_data.data is not None:
            data: NDArray[Any] = self._current_event_data.data
        else:
            data = self._empty_frame

        self._data_visualizer.visualize_data(
            data=data, array_for_visualization=self._frame_data_img
        )
        if self._ui.auto_range_cb.isChecked():
            values = data.flatten()
            values.sort()
            nvalues: int = len(values)
            self._levels_range = (
                values[nvalues // 100],
                values[nvalues - nvalues // 100],
            )

        self._image_widget.setImage(
            self._frame_data_img.T,
            autoLevels=False,
            levels=self._levels_range,
            autoRange=False,
            autoHistogramRange=False,
        )
        self._update_pixel_values()

    def _update_image_and_peaks(self) -> None:
        # Updates the image and peaks shown by the viewer.
        self._ui.current_event_index_le.setText(f"{self._current_event_index + 1}")
        self._update_image()
        self._update_peaks()
        self._update_reflections()
        if self._current_event_data.source is not None:
            status_message: str = (
                f"{self._events[self._current_event_index]}: "
                f"{self._current_event_data.source}"
            )
        else:
            status_message = f"{self._events[self._current_event_index]}"
        self.statusBar().showMessage(status_message)

    def _update_mask_image(self, mask: Optional[NDArray[Any]] = None) -> None:
        # Updates the mask image shown by the viewer.
        if mask is None and not self._ui.show_mask_cb.isChecked():
            self._mask_image.clear()
            return

        if mask is None:
            mask = self._mask
        self._mask_image.setImage(
            mask, compositionMode=QtGui.QPainter.CompositionMode_SourceOver
        )

    def _update_peaks(self) -> None:
        # Updates peaks shown by the viewer.
        if self._current_event_data.peaks is not None:
            self._ui.show_peaks_cb.setEnabled(False)
        else:
            self._ui.show_peaks_cb.setEnabled(True)
        peak_list_y_in_frame: List[float] = []
        peak_list_x_in_frame: List[float] = []
        if (
            self._current_tab == 0
            and self._ui.show_peaks_cb.isChecked()
            and self._current_event_data.peaks is not None
        ):
            peak_list = self._current_event_data.peaks
        elif (
            self._current_tab == 2
            and self._peakfinder
            and self._current_event_data.data is not None
        ):
            peak_list = self._peakfinder.find_peaks(data=self._current_event_data.data)
            self._pt_num_peaks = len(peak_list.fs)
            self._update_pt_info_label()
        else:
            peak_list = PeakList(num_peaks=0, fs=[], ss=[])

        peak_fs: float
        peak_ss: float
        for peak_fs, peak_ss in zip(
            peak_list.fs,
            peak_list.ss,
        ):
            peak_index_in_slab: int = int(round(peak_ss)) * self._data_shape[1] + int(
                round(peak_fs)
            )
            y_in_frame: float = self._flattened_visualization_pixel_map_y[
                peak_index_in_slab
            ]
            x_in_frame: float = self._flattened_visualization_pixel_map_x[
                peak_index_in_slab
            ]
            peak_list_x_in_frame.append(y_in_frame)
            peak_list_y_in_frame.append(x_in_frame)
        self._peak_canvas.setData(
            x=peak_list_y_in_frame,
            y=peak_list_x_in_frame,
            symbol="o",
            brush=(255, 255, 255, 0),
            size=8,
            pen=self._ring_pen,
            pxMode=True,
        )

    def _next_crystal(self) -> None:
        self._crystal_to_show += 1
        self._update_reflections()

    def _previous_crystal(self) -> None:
        self._crystal_to_show -= 1
        self._update_reflections()

    def _update_reflections(self) -> None:
        # Updates reflections peaks shown by the viewer.
        self._ui.next_crystal_button.setEnabled(False)
        self._ui.previous_crystal_button.setEnabled(False)
        if self._current_event_data.crystals is None:
            self._ui.show_crystals_widget.hide()
            return
        else:
            self._ui.show_crystals_widget.show()

        self._refl_canvas.clear()
        n_crystals: int = len(self._current_event_data.crystals)
        if n_crystals == 0:
            return

        if self._ui.show_no_crystals_rb.isChecked():
            crystals: List[int] = []
        elif self._ui.show_one_crystal_rb.isChecked():
            if self._crystal_to_show < 0:
                self._crystal_to_show = n_crystals - 1
            elif self._crystal_to_show > n_crystals - 1:
                self._crystal_to_show = 0
            crystals = [self._crystal_to_show]
            if n_crystals > 1:
                self._ui.next_crystal_button.setEnabled(True)
                self._ui.previous_crystal_button.setEnabled(True)
        else:
            crystals = list(range(n_crystals))

        index: int
        peak_list_y_in_frame: List[float] = []
        peak_list_x_in_frame: List[float] = []
        pen_list: List[Any] = []
        for index in crystals:
            peak_fs: float
            peak_ss: float
            for peak_fs, peak_ss in zip(
                self._current_event_data.crystals[index].fs,
                self._current_event_data.crystals[index].ss,
            ):
                peak_index_in_slab: int = int(round(peak_ss)) * self._data_shape[
                    1
                ] + int(round(peak_fs))
                y_in_frame: float = self._flattened_visualization_pixel_map_y[
                    peak_index_in_slab
                ]
                x_in_frame: float = self._flattened_visualization_pixel_map_x[
                    peak_index_in_slab
                ]
                peak_list_x_in_frame.append(y_in_frame)
                peak_list_y_in_frame.append(x_in_frame)
            pen: Any = pyqtgraph.mkPen(
                pyqtgraph.intColor(
                    index + 1,
                    n_crystals + 1,
                ),
                width=2,
            )
            pen_list.extend(
                [
                    pen,
                ]
                * self._current_event_data.crystals[index].num_peaks
            )
        self._refl_canvas.setData(
            x=peak_list_y_in_frame,
            y=peak_list_x_in_frame,
            symbol="s",
            brush=(255, 255, 255, 0),
            size=8,
            pen=pen_list,
            pxMode=True,
        )

    def _update_pixel_values(self) -> None:
        if not self._show_pixel_values:
            return
        data: NDArray[Any] = self._image_widget.image
        view_range = self._image_view.viewRange()
        pos: Tuple[int, int]
        for pos in self._pixel_value_labels:
            x: int = int(numpy.floor(view_range[0][0]) + pos[0])
            y: int = int(numpy.floor(view_range[1][0]) + pos[1])
            if 0 < int(x) < data.shape[0] and 0 < int(y) < data.shape[1]:
                pixel_value: str = f"{data[int(x)][int(y)]:.3g}"
            else:
                pixel_value = ""
            self._pixel_value_labels[pos].setText(pixel_value)
            self._pixel_value_labels[pos].setPos(x + 0.5, y + 0.5)

    def _visible_image_range_changed(self) -> None:
        pixel_size: float = self._image_item.pixelSize()[0]
        label: Any
        for label in self._pixel_value_labels.values():
            self._image_view.removeItem(label)
        self._pixel_value_labels = {}
        if pixel_size >= 50 and self._current_tab != 1:
            self._show_pixel_values = True
            view_range = self._image_view.viewRange()
            i: int
            j: int
            for i in range(int(view_range[0][1] - view_range[0][0]) + 1):
                for j in range(int(view_range[1][1] - view_range[1][0]) + 1):
                    self._pixel_value_labels[(i, j)] = pyqtgraph.TextItem(
                        anchor=(0.5, 0.5), color="g"
                    )
                    self._image_view.addItem(self._pixel_value_labels[(i, j)])
        else:
            self._show_pixel_values = False
        self._update_pixel_values()

    def _update_maskmaker_image(self) -> None:
        if self._current_tab == 1:
            # self._maskmaker_visual_mask[:] = 0
            self._maskmaker_visual_mask[
                self._flattened_visualization_pixel_map_x,
                self._flattened_visualization_pixel_map_y,
            ] = self._maskmaker_mask.ravel()
            mask_image: NDArray[Any] = numpy.zeros(
                shape=self._maskmaker_visual_mask.shape + (4,)
            )
            mask_image[:, :, 2] = self._maskmaker_visual_mask
            mask_image[:, :, 3] = self._maskmaker_visual_mask
            self._maskmaker_image.setImage(
                mask_image, compositionMode=QtGui.QPainter.CompositionMode_SourceOver
            )
        else:
            self._maskmaker_image.clear()

    def _change_mask_mode(self) -> None:
        if self._ui.toggle_rb.isChecked():
            self._mask_mode: str = "toggle"
        elif self._ui.mask_rb.isChecked():
            self._mask_mode = "mask"
        else:
            self._mask_mode = "unmask"

    def _mask_rectangular_roi(self) -> None:
        corner: Tuple[float, float] = self._rectangular_roi.pos()
        size: Tuple[float, float] = self._rectangular_roi.size()
        self._mask_original_pixels(
            numpy.where(
                (self._visualization_pixel_maps.x >= corner[0] - 0.5)
                & (self._visualization_pixel_maps.x <= corner[0] + size[0] - 0.5)
                & (self._visualization_pixel_maps.y >= corner[1] - 0.5)
                & (self._visualization_pixel_maps.y <= corner[1] + size[1] - 0.5)
            )
        )

    def _mask_curcular_roi(self) -> None:
        corner: Tuple[float, float] = self._circular_roi.pos()
        size: Tuple[float, float] = self._circular_roi.size()
        radius: float = size[0] / 2
        center: Tuple[float, float] = (
            corner[0] + radius - 0.5,
            corner[1] + radius - 0.5,
        )
        rsquared_map: NDArray[numpy.float_] = (
            self._visualization_pixel_maps.x - center[0]
        ) ** 2 + (self._visualization_pixel_maps.y - center[1]) ** 2
        self._mask_original_pixels(numpy.where(rsquared_map <= radius**2))

    def _mask_outside_histogram(self) -> None:
        self._mask_original_pixels(
            numpy.where(
                (self._current_event_data.data < self._levels_range[0])
                | (self._current_event_data.data > self._levels_range[1])
            )
        )

    def _mask_panel_edges(self) -> None:
        mask: NDArray[numpy.int_] = numpy.zeros(self._data_shape, dtype=numpy.int8)
        for panel in self._geometry.panels.values():
            min_fs: int = panel.orig_min_fs
            max_fs: int = panel.orig_max_fs
            min_ss: int = panel.orig_min_ss
            max_ss: int = panel.orig_max_ss
            mask[min_ss, min_fs : max_fs + 1] = 1
            mask[max_ss, min_fs : max_fs + 1] = 1
            mask[min_ss : max_ss + 1, min_fs] = 1
            mask[min_ss : max_ss + 1, max_fs] = 1
        self._mask_original_pixels(numpy.where(mask == 1))

    def _mask_original_pixels(
        self,
        pixels: Tuple[NDArray[numpy.int_], NDArray[numpy.int_]],
        mode: Optional[str] = None,
    ) -> None:
        if mode is None:
            mode = self._mask_mode
        if mode == "mask":
            self._maskmaker_mask[pixels] = 1
        elif mode == "unmask":
            self._maskmaker_mask[pixels] = 0
        else:
            self._maskmaker_mask[pixels] = 1 - self._maskmaker_mask[pixels]
        self._update_maskmaker_image()

    def _mask_visual_pixels(
        self,
        pixels: Tuple[NDArray[numpy.int_], NDArray[numpy.int_]],
        mode: Optional[str] = None,
    ) -> None:
        where_in_image: Tuple[NDArray[numpy.int_], NDArray[numpy.int_]] = numpy.where(
            (pixels[0] >= 0)
            & (pixels[1] >= 0)
            & (pixels[0] < self._visual_img_shape[0])
            & (pixels[1] < self._visual_img_shape[1])
        )
        pixels_in_image: Tuple[NDArray[numpy.int_], NDArray[numpy.int_]] = (
            pixels[1][where_in_image],
            pixels[0][where_in_image],
        )
        visual_mask: NDArray[numpy.int_] = numpy.zeros(
            self._visual_img_shape, dtype=numpy.int8
        )
        visual_mask[pixels_in_image] = 1
        self._mask_original_pixels(
            numpy.where(
                visual_mask[
                    self._flattened_visualization_pixel_map_y,
                    self._flattened_visualization_pixel_map_x,
                ].reshape(self._data_shape)
                == 1
            ),
            mode=mode,
        )

    def _dilate_mask(self) -> None:
        for panel in self._geometry.panels.values():
            min_fs: int = panel.orig_min_fs
            max_fs: int = panel.orig_max_fs
            min_ss: int = panel.orig_min_ss
            max_ss: int = panel.orig_max_ss
            self._maskmaker_mask[min_ss : max_ss + 1, min_fs : max_fs + 1] = (
                binary_dilation(
                    self._maskmaker_mask[min_ss : max_ss + 1, min_fs : max_fs + 1]
                ).astype(self._maskmaker_mask.dtype)
            )
        self._update_maskmaker_image()

    def _erode_mask(self) -> None:
        for panel in self._geometry.panels.values():
            min_fs: int = panel.orig_min_fs
            max_fs: int = panel.orig_max_fs
            min_ss: int = panel.orig_min_ss
            max_ss: int = panel.orig_max_ss
            self._maskmaker_mask[min_ss : max_ss + 1, min_fs : max_fs + 1] = (
                binary_erosion(
                    self._maskmaker_mask[min_ss : max_ss + 1, min_fs : max_fs + 1]
                ).astype(self._maskmaker_mask.dtype)
            )
        self._update_maskmaker_image()

    def _clear_mask(self) -> None:
        self._maskmaker_mask[:] = 0
        self._update_maskmaker_image()

    def _add_mask_from_file(self) -> None:
        if self._mask_filename and pathlib.Path(self._mask_filename).is_file():
            path: pathlib.Path = pathlib.Path(self._mask_filename)
        else:
            path = pathlib.Path.cwd()
        filename: str = QtWidgets.QFileDialog().getOpenFileName(
            self, "Select mask file", str(path), filter="*.h5"
        )[0]
        if filename:
            h5file: Any
            with h5py.File(filename, "r") as h5file:
                mask: NDArray[numpy.int_] = h5file["/data/data"][()]
                self._mask_original_pixels(numpy.where(mask == 0), mode="mask")

    def _start_brush(self) -> None:
        if self._ui.brush_button.isChecked():
            QtWidgets.QApplication.setOverrideCursor(QtCore.Qt.CrossCursor)
            self._brush_image = pyqtgraph.ImageItem(
                numpy.zeros((self._visual_img_shape[0], self._visual_img_shape[1], 4))
            )
            self._brush_image.setZValue(10)
            self._image_view.addItem(self._brush_image)
            self._change_brush_size()
        else:
            self._discard_brush()

    def _generate_brush_kernel(self) -> NDArray[numpy.float_]:
        size: int = self._ui.brush_size_sb.value()
        radius: float = size / 2.0
        corner: float = (size - 1) / 2.0
        xgrid: NDArray[numpy.float_]
        ygrid: NDArray[numpy.float_]
        xgrid, ygrid = numpy.ogrid[-corner : size - corner, -corner : size - corner]
        kernel: NDArray[numpy.float_] = numpy.zeros((size, size, 4))
        kernel[:, :, 0][xgrid**2 + ygrid**2 < radius**2] = 1
        kernel[:, :, 3][xgrid**2 + ygrid**2 < radius**2] = 1
        return kernel

    def _change_brush_size(self) -> None:
        if self._ui.brush_button.isChecked():
            kernel: NDArray[numpy.float_] = self._generate_brush_kernel()
            self._brush_image.setLevels([0, 1])
            self._brush_image.setDrawKernel(
                kernel,
                mask=kernel,
                center=(kernel.shape[0] // 2, kernel.shape[1] // 2),
                mode="set",
            )
        else:
            pass

    def _add_brush(self) -> None:
        if not self._ui.brush_button.isChecked():
            return
        self._mask_visual_pixels(
            numpy.where(self._brush_image.image[:, :, 0] > 0), mode="mask"
        )
        self._discard_brush()

    def _discard_brush(self) -> None:
        QtWidgets.QApplication.restoreOverrideCursor()
        self._brush_image.clear()
        if self._ui.brush_button.isChecked():
            self._ui.brush_button.toggle()

    def _mouse_clicked(self, click: Any) -> None:
        if self._current_tab != 1 or self.brush_button.isChecked():
            return
        scene_pos: Any = self._image_item.mapFromScene(click.pos())
        self._mask_visual_pixels(
            (numpy.array([int(scene_pos.x())]), numpy.array([int(scene_pos.y())]))
        )

    def _save_mask(self) -> None:
        if self._mask_filename and pathlib.Path(self._mask_filename).is_file():
            mask_filename: pathlib.Path = pathlib.Path(self._mask_filename)
            path: pathlib.Path = mask_filename.parent
            name: str = f"{mask_filename.stem}-new{mask_filename.suffix}"
        else:
            path = pathlib.Path.cwd()
            name = "mask-new.h5"
        filename: str = QtWidgets.QFileDialog().getSaveFileName(
            self, "Select mask file", str(path / name), filter="*.h5"
        )[0]
        if filename:
            logger.info(f"Saving new mask to {filename}.")
            with h5py.File(filename, "w") as fh:
                fh.create_dataset("/data/data", data=(1 - self._maskmaker_mask))

    def _load_peakfinder_parameters(self) -> None:
        filename: str = QtWidgets.QFileDialog().getOpenFileName(
            self,
            "Select OM configuration file",
            str(self._pt_config_path),
            filter="*.yaml",
        )[0]
        if not filename:
            return

        logger.info(f"Loading peakfinder parameters from {filename}")
        self._pt_config_path = pathlib.Path(filename)

        self._yaml: ruamel.yaml.YAML = ruamel.yaml.YAML(typ="jinja2")
        self._yaml.indent(mapping=2, sequence=1, offset=2)
        self._yaml.preserve_quotes = True
        with open(filename, "r") as fh:
            self._pt_config: Dict[str, Any] = self._yaml.load(fh)

        pf8_config: Dict[str, Any] = self._pt_config[
            "peakfinder8_peak_detection"
        ].copy()
        pf8_config["min_num_peaks_for_hit"] = self._pt_config["crystallography"][
            "min_num_peaks_for_hit"
        ]
        self._fill_peakfinder_parameters(pf8_config)
        if not pathlib.Path(pf8_config["bad_pixel_map_filename"]).is_file():
            if self._ui.show_mask_cb.isEnabled():
                pf8_config["bad_pixel_map_filename"] = self._mask_filename
                pf8_config["bad_pixel_map_hdf5_path"] = self._mask_hdf5_path
                self._pt_mask = self._mask
            else:
                pf8_config["bad_pixel_map_filename"] = None

        for input_widget in self._pt_inputs:
            input_widget.setEnabled(True)
        self._ui.pt_load_mask_button.setEnabled(True)
        self._ui.save_config_button.setEnabled(True)

        self._peakfinder = Peakfinder8PeakDetection(
            radius_pixel_map=self._radius_pixel_map,
            layout_info=self._detector_layout_info,
            parameters=pf8_config,
        )
        self._update_mask_image(self._pt_mask)
        self._update_peaks()

    def _save_peakfinder_parameters(self) -> None:
        pf8_config: Dict[str, Any] = self._pt_config["peakfinder8_peak_detection"]
        pf8_config["adc_threshold"] = float(self._ui.pt_adc_thresh_le.text())
        pf8_config["minimum_snr"] = float(self._ui.pt_minimum_snr_le.text())
        pf8_config["min_res"] = int(self._ui.pt_min_res_le.text())
        pf8_config["max_res"] = int(self._ui.pt_max_res_le.text())
        pf8_config["min_pixel_count"] = int(self._ui.pt_min_pixel_count_le.text())
        pf8_config["max_pixel_count"] = int(self._ui.pt_max_pixel_count_le.text())
        pf8_config["local_bg_radius"] = int(self._ui.pt_local_bg_radius_le.text())
        self._pt_config["crystallography"]["min_num_peaks_for_hit"] = int(
            self._ui.pt_min_peaks_le.text()
        )
        new_config_path: pathlib.Path = self._pt_config_path.parent / (
            self._pt_config_path.stem + "-new.yaml"
        )
        filename: str = QtWidgets.QFileDialog().getSaveFileName(
            self, "Save new config file", str(new_config_path), filter="*.yaml"
        )[0]
        if not filename:
            return

        reply: Any = QtWidgets.QMessageBox.information(
            self,
            "Warning",
            "Bad pixel map will not be saved in the new configuration file.",
            QtWidgets.QMessageBox.Ok | QtWidgets.QMessageBox.Cancel,
        )
        if reply == QtWidgets.QMessageBox.Cancel:
            return

        logger.info(f"Saving new peakfinder parameters to {filename}")
        fh: TextIO
        with open(filename, "w") as fh:
            self._yaml.dump(self._pt_config, fh)
            self._pt_config_path = pathlib.Path(filename)

    def _load_mask_for_peakfinder(self) -> None:
        if self._mask_filename and pathlib.Path(self._mask_filename).is_file():
            path: pathlib.Path = pathlib.Path(self._mask_filename)
        else:
            path = pathlib.Path.cwd()
        filename: str = QtWidgets.QFileDialog().getOpenFileName(
            self, "Select mask file", str(path), filter="*.h5"
        )[0]
        if filename:
            hdf5_path: str = QtWidgets.QInputDialog.getText(
                self,
                "Mask HDF5 path",
                "Enter the path to the mask dataset in the file:",
                text="/data/data",
            )[0]
            if not hdf5_path:
                return
        else:
            return

        mask_file: Any
        with h5py.File(filename, "r") as mask_file:
            if hdf5_path not in mask_file:
                logger.error(f"Dataset {hdf5_path} not found in {filename}.")
                return
            mask: NDArray[numpy.int_] = mask_file[hdf5_path][()]
            self._pt_mask = self._create_mask_image(mask)
        self._update_mask_image(self._pt_mask)
        if self._peakfinder is not None:
            self._peakfinder.set_bad_pixel_map(mask)
        self._update_peaks()

    def _update_peakfinder_parameters(self, input_widget: Any) -> None:
        if input_widget.text() == "":
            input_widget.setText("0")

        if input_widget.objectName() == "pt_min_peaks_le":
            self._update_pt_info_label()
            return

        pf_arg: str = input_widget.objectName()[3:-3]
        func: Callable[..., Any] = getattr(self._peakfinder, "set_" + pf_arg)
        if pf_arg in ("adc_thresh", "minimum_snr"):
            func(**{pf_arg: float(input_widget.text())})
        else:
            func(**{pf_arg: int(input_widget.text())})
        self._update_peaks()

    def _update_pt_info_label(self) -> None:
        self._ui.pt_info_label.setText(f"Found {self._pt_num_peaks} peaks.")
        min_peaks: int = int(self._ui.pt_min_peaks_le.text())
        if self._pt_num_peaks >= min_peaks:
            self._ui.pt_info_label.setStyleSheet("color: green")
        else:
            self._ui.pt_info_label.setStyleSheet("color: red")

    def _fill_peakfinder_parameters(self, parameters: Dict[str, Any]) -> None:
        self._ui.pt_adc_thresh_le.setText(str(parameters["adc_threshold"]))
        self._ui.pt_minimum_snr_le.setText(str(parameters["minimum_snr"]))
        self._ui.pt_min_res_le.setText(str(parameters["min_res"]))
        self._ui.pt_max_res_le.setText(str(parameters["max_res"]))
        self._ui.pt_min_pixel_count_le.setText(str(parameters["min_pixel_count"]))
        self._ui.pt_max_pixel_count_le.setText(str(parameters["max_pixel_count"]))
        self._ui.pt_local_bg_radius_le.setText(str(parameters["local_bg_radius"]))
        self._ui.pt_min_peaks_le.setText(str(parameters["min_num_peaks_for_hit"]))

    def _move_geometry_rois(self, moved_roi: Any, dx: float = 0, dy: float = 0) -> None:
        if self._current_tab != 3:
            return
        if moved_roi is not None:
            c: Any = moved_roi.parentBounds().center()
        else:
            c = self._geometry_rois[0].parentBounds().center() - QtCore.QPointF(dx, dy)
        for roi in self._geometry_rois:
            if roi is not moved_roi:
                roi.setPos(c - roi.size() / 2)
        self._update_geometry_labels()

    def _update_geometry_labels(self) -> None:
        self._geometry_roi_radii = [roi.size()[0] / 2 for roi in self._geometry_rois]
        center: Any = self._geometry_rois[0].parentBounds().center()
        self._geometry_shift = (
            self._img_center_x - center.x(),
            self._img_center_y - center.y(),
        )

        self._ui.ds_rings_label.setText(
            "Rings: "
            + " ".join((f"{r:.1f}" for r in sorted(self._geometry_roi_radii)))
            + " pixels"
        )
        self._ui.ds_shift_label.setText(
            "Detector shift: "
            + f"{self._geometry_shift[0]:.2f} {self._geometry_shift[1]:.2f} pixels"
        )

    def _save_geometry(self) -> None:
        new_geometry_lines: List[str] = []
        for line in self._geometry_lines:
            if not line.startswith(";"):
                try:
                    key, value = (item.strip() for item in line.split("="))
                    if key.split("/")[-1] == "corner_x":
                        corner_x: float = float(value) + self._geometry_shift[0]
                        line = f"{key} = {corner_x}\n"
                    elif key.split("/")[-1] == "corner_y":
                        corner_y: float = float(value) + self._geometry_shift[1]
                        line = f"{key} = {corner_y}\n"
                except ValueError:
                    pass
            new_geometry_lines.append(line)

        filename: str = QtWidgets.QFileDialog().getSaveFileName(
            self, "Save new geometry file", str(self._geometry_path), filter="*.geom"
        )[0]
        if not filename:
            return
        with open(filename, "w") as fh:
            fh.writelines(new_geometry_lines)

    def keyPressEvent(self, event: Any) -> None:
        if self._current_tab != 3:
            return

        if event.key() == QtCore.Qt.Key_Up:
            self._move_geometry_rois(None, dy=-1)
        elif event.key() == QtCore.Qt.Key_Down:
            self._move_geometry_rois(None, dy=1)
        elif event.key() == QtCore.Qt.Key_Left:
            self._move_geometry_rois(None, dx=1)
        elif event.key() == QtCore.Qt.Key_Right:
            self._move_geometry_rois(None, dx=-1)


def _get_hdf5_retrieval_parameters(geometry_filename: str) -> Dict[str, Any]:
    # This function is used internally to get parameters for hdf5 data retrieval from
    # the geometry file.
    geometry: Detector
    beam: Beam
    fh: TextIO
    with open(geometry_filename, "r") as fh:
        geometry, beam, __ = _read_crystfel_geometry_from_text(
            text_lines=fh.readlines()
        )
    first_panel: str = list(geometry.panels.keys())[0]
    return {
        "hdf5_data_path": geometry.panels[first_panel].data,
        "clen_path": geometry.panels[first_panel].clen_from,
        "photon_energy_path": beam.photon_energy_from,
    }


def _parse_config_file(filename: pathlib.Path) -> Dict[str, Any]:
    fh: TextIO
    with open(filename) as fh:
        config: Dict[str, Any] = yaml.safe_load(fh)
    return config


def _get_geometry_file_contents(stream_filename: str) -> List[str]:
    # Gets contents of the geometry file from stream file.
    geometry_lines: List[str] = []
    reading_geometry: bool = False
    fh: TextIO
    with open(stream_filename, "r") as fh:
        line: str
        for line in fh:
            if line.startswith("----- End geometry file -----"):
                break
            elif reading_geometry:
                geometry_lines.append(line)
            elif line.startswith("----- Begin geometry file -----"):
                reading_geometry = True
    return geometry_lines


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.argument(  # type: ignore
    "input_files",
    nargs=-1,
    type=click.Path(exists=True),
    required=True,
    metavar="INPUT_FILE(S)",
)
@click.option(  # type: ignore
    "--input-type",
    "-i",
    "input_type",
    type=click.Choice(
        ["hdf5", "om", "dir", "stream"],
        case_sensitive=False,
    ),
    default="hdf5",
    help="type of the input sources, default: hdf5",
)
@click.option(  # type: ignore
    "--geometry",
    "-g",
    "geometry_filename",
    nargs=1,
    type=click.Path(exists=True),
    required=False,
    help="CrystFEL geometry file, required for all input types except stream files",
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
@click.option(  # type: ignore
    "--maskmaker",
    is_flag=True,
    default=False,
    help="open the maskmaker tab, default: False",
)
def main(
    input_files: List[str],
    input_type: str,
    geometry_filename: str,
    mask_filename: Optional[str],
    hdf5_mask_path: str,
    hdf5_data_path: Optional[str],
    hdf5_peaks_path: Optional[str],
    om_source: str,
    om_config: str,
    om_peaks_file: Optional[str],
    maskmaker: bool,
) -> None:
    """
    Cheetah Viewer. The viewer displays images from various sources applying detector
    geometry from the provided geometry file in CrystFEL format. The viewer can
    optionally display positions of the detected peaks when they are available. It can
    also show mask if the mask file is provided either as a command line argument or as
    an entry in the geometry file.

    The viewer can retrieve images from the following sources:

    1) HDF5 files ('--input-type=hdf5', default option). To display images stored in
    single- or multi-event HDF5 files one or several .h5 or .cxi files must be provided
    as INPUT_FILE(S). Geometry file ('--geometry' / '-g') must also be provided.
    Additionally, '--hdf5-data-path' / '-p' option may be used to specify the dataset
    name where the images are stored within the HDF5 files. Otherwise, the path
    specified in the geometry file will be used. '--hdf5-peaks-path' / '-p' option may
    be used to specify where the peaks dataset is stored in the HDF5 files. If this
    option is not set, detected peak positions won't be shown.

    \b
    Usage example: cheetah_viewer.py data_*.h5 -g current.geom -d /data/data -p /data/peaks

    2) OM data retrieval layer ('--input-type=om'). To display images retrieved by OM
    one text file containing a list of OM event IDs must be provided as INPUT_FILE.
    Geometry file ('--geometry' / '-g') and both OM source string ('--om-source / '-s')
    and OM config file ('--om-config' / '-c') must also be provided. Additionally,
    Cheetah peak list file ('--om-peaks-file') can be used to retrieve information
    about detected peaks. If peak list file is not specified, peaks will be detected
    using peakfinder8 parameters from the provided config file.

    \b
    Usage example: cheetah_viewer.py -i om hits.lst -g current.geom -s exp=mfxlz0420:run=141 -c monitor.yaml --om-peaks-file=peaks.txt

    3) Cheetah hdf5 directories ('--input-type=dir'). To display images from one or
    several runs processed by Cheetah, hdf5 directories of these runs must be provided
    as INPUT_FILE(S). Geometry file ('--geometry' / '-g') must also be provided.

    \b
    Usage example: cheetah_viewer.py -i dir cheetah/hdf5/r*-lyso -g current.geom

    4) CrystFEL stream files ('--input-type=stream'). To display images processed by
    CrystFEL program indexamajig, as well as detected peaks and predicted reflection
    positions, one or several CrystFEL stream files must be provided as INPUT_FILE(S).
    Geometry file ('--geometry' / '-g') may be provided but is not required - when not
    provided the contents of the geometry file will be extracted from the input stream
    file.

    \b
    Usage example: cheetah_viewer.py -i stream lyso_*.stream
    """
    # Set up logging.
    logging.config.dictConfig(logging_config)

    geometry_lines: List[str] = []
    if input_type != "stream" and geometry_filename is None:
        logger.error(
            f"Missing option '--geometry' / '-g'."
            f"Geometry file is required for input type '{input_type}'."
        )
        sys.exit(1)

    pt_config_path: Optional[str] = None
    geometry_path: Optional[str] = None
    if input_type == "om":
        logger.info("Activating frame retrieval from OM data retrieval layer.")
        if not pathlib.Path(om_config).is_file():
            logger.error(
                f"Invalid value for '--om-config' / '-c': Path {om_config} "
                f"does not exist."
            )
            sys.exit(1)
        pt_config_path = om_config
        geometry_path = geometry_filename

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
    elif input_type == "dir":
        sources: List[str] = []
        parameters = {
            "om_sources": {},
            "om_configs": {},
            "peak_lists": {},
        }
        h5_files: List[str] = []
        input_path: str
        for input_path in input_files:
            dir: pathlib.Path = pathlib.Path(input_path)
            if not dir.is_dir():
                logger.warning(f"Skipping input source {dir}: is not a directory.")
                continue
            process_config: pathlib.Path = dir / "process.config"
            if process_config.is_file():
                config: Dict[str, Any] = _parse_config_file(process_config)
                source_string: str = config["Process script template data"]["om_source"]
                config_file: pathlib.Path = pathlib.Path(
                    config["Process script template data"]["om_config"]
                )
            else:
                logger.warning(
                    f"Skipping input source {dir}: {process_config} file not found."
                )
                continue

            pt_config_path = config["Processing config"]["config_template"]
            geometry_path = config["Processing config"]["geometry"]

            hits_file: pathlib.Path = dir / "hits.lst"
            peaks_file: pathlib.Path = dir / "peaks.txt"

            if config_file.is_file() and hits_file.is_file() and peaks_file.is_file():
                sources.append(str(hits_file))
                parameters["om_sources"][str(hits_file)] = source_string
                parameters["om_configs"][str(hits_file)] = config_file
                parameters["peak_lists"][str(hits_file)] = peaks_file

            filename: pathlib.Path
            for filename in dir.glob("*"):
                if filename.suffix in (".h5", ".cxi") and not filename.name.endswith(
                    "sum.h5"
                ):
                    h5_files.append(str(filename))

        if len(sources) > 0:
            logger.info("Loading hits from the following files:")
            source: str
            for source in sources:
                logger.info(f"  - {source}")

        frame_retrieval = OmRetrieval(sources, parameters)
        if len(frame_retrieval.get_event_list()) == 0:
            if len(h5_files) > 0:
                logger.info(
                    "Couldn't retrieve any images from selected runs using OM frame "
                    "retrieval. Trying to load images from HDF5 files."
                )
                parameters = _get_hdf5_retrieval_parameters(geometry_filename)
                # TODO: get peaks path from the geometry file or monitor.yaml
                parameters["hdf5_peaks_path"] = "/entry_1/result_1"
                frame_retrieval = H5FilesRetrieval(h5_files, parameters)
            else:
                logger.info(
                    "Couldn't retrieve any images from selected runs using OM frame "
                    "retrieval and there's no .h5 or .cxi files in the selected "
                    "directories yet."
                )
    elif input_type == "stream":
        logger.info("Activating frame retrieval from CrystFEL stream files.")
        stream_filename: str
        if geometry_filename is None:
            for stream_filename in input_files:
                geometry_lines = _get_geometry_file_contents(stream_filename)
                if len(geometry_lines) > 0:
                    logger.info(f"Using geometry file contents from {stream_filename}.")
                    break
            if len(geometry_lines) == 0:
                logger.error(
                    "Couldn't extract geometry file contents from the input stream "
                    "files. Please provide a geometry file ('--geometry' / '-g')."
                )
                sys.exit(1)

        frame_retrieval = StreamRetrieval(input_files, {})
    else:
        logger.info("Activating frame retrieval from HDF5 files.")
        parameters = _get_hdf5_retrieval_parameters(geometry_filename)
        if hdf5_data_path:
            parameters["hdf5_data_path"] = hdf5_data_path
        if hdf5_peaks_path:
            parameters["hdf5_peaks_path"] = hdf5_peaks_path

        frame_retrieval = H5FilesRetrieval(input_files, parameters)
        geometry_path = geometry_filename

    if geometry_filename:
        fh: TextIO
        with open(geometry_filename) as fh:
            geometry_lines = fh.readlines()

    if maskmaker:
        open_tab: int = 1
    else:
        open_tab = 0

    sys.stdout.flush()
    if len(frame_retrieval.get_event_list()) == 0:
        return

    app: Any = QtWidgets.QApplication(sys.argv)
    _ = Viewer(
        frame_retrieval,
        geometry_lines,
        mask_filename,
        hdf5_mask_path,
        open_tab,
        pt_config_path,
        geometry_path,
    )
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

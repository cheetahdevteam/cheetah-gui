"""
Cheetah GUI.

This module contains the implementation of the main Cheetah GUI.
"""

import csv
import os
import pathlib
import sys
from operator import itemgetter
from pathlib import Path
from typing import Any, Dict, List, NamedTuple, Optional, Set, TextIO, Tuple, Union

import click  # type: ignore
from ansi2html import Ansi2HTMLConverter  # type: ignore
from PyQt5 import QtCore, QtGui, QtWidgets, uic  # type: ignore

try:
    from typing import Literal
except:
    from typing_extensions import Literal  # type: ignore

import logging
import logging.config

from om.lib.exceptions import OmConfigurationFileSyntaxError
from om.lib.files import load_configuration_parameters
from pydantic import BaseModel, Field, ValidationError

from cheetah import __file__ as cheetah_src_path
from cheetah.crawlers.base import Crawler, TableRow
from cheetah.dialogs import process_dialogs, setup_dialogs
from cheetah.experiment import CheetahExperiment, ExperimentConfig
from cheetah.process import TypeProcessingConfig
from cheetah.utils.logging import LoggingPopen, QtHandler, logging_config

logger: logging.Logger = logging.getLogger("cheetah")


class _CrystallographyParameters(BaseModel):
    geometry_file: str


class _Peakfinder8Parameters(BaseModel):
    bad_pixel_map_filename: Optional[str] = Field(default=None)


class _MonitorParameters(BaseModel):
    crystallography: _CrystallographyParameters
    peakfinder8_peak_detection: _Peakfinder8Parameters


class _CrawlerRefresher(QtCore.QObject):  # type: ignore
    # This class is used internally running in a separate Qt thread to periodically
    # update Cheetah GUI run table information using Cheetah crawler.

    finished: Any = QtCore.pyqtSignal()

    def __init__(self, crawler: Crawler) -> None:
        super(_CrawlerRefresher, self).__init__()
        self._crawler: Crawler = crawler

    def refresh(self) -> None:
        self._crawler.update()
        self.finished.emit()


class CrawlerGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    scan_finished: Any = QtCore.pyqtSignal()
    """
    Qt signal emitted when crawler finishes scanning directories.
    """

    def __init__(self, experiment: CheetahExperiment, parent: Any = None) -> None:
        """
        Cheetah Crawler Gui.

        This class implements a graphical user interface for Cheetah Crawler. It stores
        an instance of [Cheetah Crawler][cheetah.crawlers.base.Crawler] class in a
        separate Qt thread and periodically calls its
        [update][cheetah.crawlers.base.Crawler.update] method, which scans raw and
        processed data directories and saves accumulated information in a CSV file. The
        GUI also implements a "Refresh" button which can be used to trigger the update
        manually.

        Arguments:

            experiment: An instance of
                [CheetahExperiment][cheetah.experiment.Experiment] class which creates
                the crawler.

            parent: Parent QtWidget. Defaults to None.
        """
        super(CrawlerGui, self).__init__(parent)
        self.parent: Any = parent
        self.resize(300, 50)
        self.setWindowTitle("Cheetah Crawler")

        self._experiment: CheetahExperiment = experiment
        self._crawler: Crawler = self._experiment.start_crawler()

        self._raw_scan_enable_button: Any = QtWidgets.QCheckBox("Scan raw directory")
        self._raw_scan_enable_button.setChecked(
            self._crawler.raw_directory_scan_is_enabled()
        )
        self._raw_scan_enable_button.stateChanged.connect(self._update_crawler_config)

        self._proc_scan_enable_button: Any = QtWidgets.QCheckBox("Scan hdf5 directory")
        self._proc_scan_enable_button.setChecked(
            self._crawler.proc_directory_scan_is_enabled()
        )
        self._proc_scan_enable_button.stateChanged.connect(self._update_crawler_config)

        self._refresh_button: Any = QtWidgets.QPushButton("Refresh")
        self._refresh_button.clicked.connect(self._refresh)
        self._status_label: Any = QtWidgets.QLabel()

        hlayout: Any = QtWidgets.QHBoxLayout()
        hlayout.addWidget(self._refresh_button)
        hlayout.addWidget(self._status_label)
        layout: Any = QtWidgets.QVBoxLayout()
        layout.addWidget(self._raw_scan_enable_button)
        layout.addWidget(self._proc_scan_enable_button)
        layout.addLayout(hlayout)
        self._central_widget = QtWidgets.QWidget()
        self._central_widget.setLayout(layout)
        self.setCentralWidget(self._central_widget)

        self._refresher = _CrawlerRefresher(self._crawler)
        self._refresh_thread: Any = QtCore.QThread()
        self._refresher.moveToThread(self._refresh_thread)

        self._refresh_thread.started.connect(self._refresher.refresh)
        self._refresher.finished.connect(self._refresh_finished)
        self._refresher.finished.connect(self._refresh_thread.quit)

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh)

        self._refresh()

    def _update_crawler_config(self) -> None:
        # Changes crawler scanning parameters and updates crawler.config file.
        self._crawler.set_raw_directory_scan_enabled(
            self._raw_scan_enable_button.isChecked()
        )
        self._crawler.set_proc_directory_scan_enabled(
            self._proc_scan_enable_button.isChecked()
        )
        self._experiment.write_crawler_config()

    def _refresh(self) -> None:
        # Starts crawler update.
        self._refresh_button.setEnabled(False)
        self._raw_scan_enable_button.setEnabled(False)
        self._proc_scan_enable_button.setEnabled(False)
        self._status_label.setText("Scanning files")
        self._refresh_thread.start()

    def _refresh_finished(self) -> None:
        # Finishes crawler update.
        self._refresh_button.setEnabled(True)
        self._raw_scan_enable_button.setEnabled(True)
        self._proc_scan_enable_button.setEnabled(True)
        self._status_label.setText("Ready")
        self._refresh_timer.start(20000)
        self.scan_finished.emit()

    def closeEvent(self, event: Any) -> None:
        """
        Let the parent know about the GUI closing and accept.

        This function is called when the GUI window is closed. It lets the main Cheetah
        GUI window know about it.
        """
        self._refresh_timer.stop()
        self.parent._crawler_gui_closed()
        event.accept()


class ProcessThread(QtCore.QThread):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        experiment: CheetahExperiment,
        runs: List[str],
        config: TypeProcessingConfig,
        streaming: bool,
        hit_files: Optional[Dict[str, pathlib.Path]] = None,
    ) -> None:
        """
        Process Thread.

        This class is initialized when Cheetah processing is triggered for a list of
        runs from Cheetah GUI, creating a separate Qt thread. When the thread is
        started it calls the [run][cheetah.gui.ProcessThread.run] function, which
        launches processing for each run in the list.

        Arguments:
            experiment: An instance of
                [CheetahExperiment][cheetah.experiment.Experiment] class which launches
                data processing.

            runs: A list of run IDs which should be processed.

            config: A [TypeProcessingConfig][cheetah.process.TypeProcessingConfig]
                dictionary containing processing configuration parameteres. This
                argument will be passed to
                [CheetahExperiment.process_run][cheetah.experiment.Experiment.process_run]
                function.

            streaming: Whether to save hits to files or stream them to CrystFEL.
        """
        super(ProcessThread, self).__init__()
        self._experiment: CheetahExperiment = experiment
        self._runs: List[str] = runs
        self._config: TypeProcessingConfig = config
        self._streaming: bool = streaming
        self._hit_files: Optional[Dict[str, pathlib.Path]] = hit_files

    def run(self) -> None:
        """
        Process runs.

        This function is called when ProcessThread is started. It calls Cheetah
        Experiment [process_runs][cheetah.experiment.Experiment.process_runs] function.
        """
        self._experiment.process_runs(
            self._runs, self._config, self._streaming, self._hit_files
        )


class TextFileViewer(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, filename: str, parent: Any = None) -> None:
        """
        Text File Viewer.

        This class implements a simple text file viewer. It is called by the main
        Cheetah GUI to display contents of the log files.

        Arguments:

            filename: The name of the text file to display.

            parent: Parent QtWidget. Defaults to None

        """
        super(TextFileViewer, self).__init__(parent)
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self.setWindowTitle(f"{filename}")
        self.parent: Any = parent

        self._filename: pathlib.Path = pathlib.Path(filename)
        self._converter = Ansi2HTMLConverter()

        self._reload_button: Any = QtWidgets.QPushButton("Reload")
        self._reload_button.clicked.connect(self._update)
        self._text_edit: Any = QtWidgets.QTextEdit()
        self._text_edit.setReadOnly(True)

        layout: Any = QtWidgets.QVBoxLayout()
        layout.addWidget(self._text_edit)
        layout.addWidget(self._reload_button)
        self._central_widget = QtWidgets.QWidget()
        self._central_widget.setLayout(layout)
        self.setCentralWidget(self._central_widget)
        self.resize(800, 600)
        self._update()
        self.show()

    def _update(self) -> None:
        # Displays contents of the input file.
        self._text_edit.clear()
        if pathlib.Path(self._filename).exists():
            fh: TextIO
            with open(self._filename, "r") as fh:
                self._text_edit.setHtml(self._converter.convert(fh.read()))
                self._text_edit.moveCursor(QtGui.QTextCursor.End)
        else:
            self._text_edit.setPlainText(f"File {self._filename} doesn't exist.")


class _TreeItemDelegate(QtWidgets.QStyledItemDelegate):
    # Color delegate for the tree view item.
    def __init__(
        self, colors: Dict[str, Any], columns_to_paint: List[int], parent=None
    ):
        super(_TreeItemDelegate, self).__init__(parent)
        self._colors: Dict[str, Any] = colors
        self._columns_to_paint: List[int] = columns_to_paint

    def paint(self, painter, option, index):
        if index.parent() == QtCore.QModelIndex() or index.column() > 1:
            column = index.column()
            if column in self._columns_to_paint:
                value = index.model().data(index)
                if value in self._colors.keys():
                    painter.fillRect(option.rect, self._colors[value])
            painter.setPen(QtGui.QColor(200, 200, 200))
            # painter.drawRect(option.rect)
            painter.drawLine(option.rect.bottomLeft(), option.rect.bottomRight())
            painter.drawLine(option.rect.topLeft(), option.rect.topRight())
            painter.drawLine(option.rect.topRight(), option.rect.bottomRight())
            painter.drawLine(option.rect.topLeft(), option.rect.bottomLeft())

        super(_TreeItemDelegate, self).paint(painter, option, index)


class _SelectedRows(NamedTuple):
    # A tuple of selected rows, run_ids, processing directories and data model indices.
    rows: Tuple[float]
    runs: Tuple[str]
    proc_dirs: Tuple[str]
    indices: Tuple[QtCore.QModelIndex]


class CheetahGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, command: bool = False) -> None:
        """
        Cheetah GUI.

        This class implements the main Cheetah GUI window. It consists of a table of
        runs used to coordinate processing and simplify the viewing and monitoring of
        output. When started it creates an instance of
        [CheetahExperiment][cheetah.experiment.Experiment] class which is then used to
        update the table and launch data processing.

        Attributes:

            command: Whether to enable command operations and start the crawler on
                start-up. Defaults to False.
        """
        super(CheetahGui, self).__init__()
        self._ui: Any = uic.loadUi(
            (pathlib.Path(cheetah_src_path) / "../ui_src/cheetahgui.ui").resolve(), self
        )
        self.setWindowIcon(
            QtGui.QIcon(
                str((pathlib.Path(cheetah_src_path) / "../ui_src/icon.svg").resolve())
            )
        )
        self.show()

        # Set up log view
        self._logview: Any = self._ui.log_textedit
        handler: logging.Handler = QtHandler(self)
        handler.new_record.connect(self._logview.appendPlainText)
        logger.addHandler(handler)

        # Load experiment
        self._select_experiment()
        self.setWindowTitle(f"Cheetah GUI: {self.experiment.get_working_directory()}")
        self._crawler_csv_filename: pathlib.Path = (
            self.experiment.get_crawler_csv_filename()
        )

        # Set up table
        self._table_column_names: List[str] = list(TableRow.__annotations__.keys())
        self._proc_dir_column: int = self._table_column_names.index("H5Directory")
        self._cheetah_status_column: int = self._table_column_names.index("Cheetah")
        self._dataset_tag_column: int = self._table_column_names.index("Dataset")
        self._num_columns: int = len(self._table_column_names)

        self._tree: Any = self._ui.status_treeview
        self._data_model: Any = QtGui.QStandardItemModel()
        self._data_model.setHorizontalHeaderLabels(self._table_column_names)
        self._tree.setModel(self._data_model)
        self._tree.header().setDefaultSectionSize(self.width() // self._num_columns)

        self._status_colors: Dict[str, Any] = {
            "---": QtGui.QColor(255, 255, 255),
            "Submitting": QtGui.QColor(255, 165, 0),
            "Submitted": QtGui.QColor(255, 255, 100),
            "Copying": QtGui.QColor(255, 255, 100),
            "Restoring": QtGui.QColor(255, 255, 100),
            "Incomplete": QtGui.QColor(255, 255, 100),
            "Started": QtGui.QColor(0, 255, 255),
            "Not finished": QtGui.QColor(0, 255, 255),
            "Ready": QtGui.QColor(200, 255, 200),
            "Finished": QtGui.QColor(200, 255, 200),
            "Dark ready": QtGui.QColor(70, 130, 180),
            "Cancelling": QtGui.QColor(255, 165, 165),
            "Cancelled": QtGui.QColor(255, 200, 200),
            "Error": QtGui.QColor(255, 100, 100),
            "Removing": QtGui.QColor(100, 100, 255),
        }
        columns_to_paint: List[int] = [
            i
            for i, name in enumerate(self._table_column_names)
            if name in ("Rawdata", "Cheetah")
        ]
        self._tree.setItemDelegate(
            _TreeItemDelegate(self._status_colors, columns_to_paint)
        )

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh_table)

        self._refresh_table()
        self.crawler_window: Optional[CrawlerGui] = None

        # Connect front panel buttons to actions
        self._ui.action_refresh_table.triggered.connect(self._refresh_table)
        self._ui.action_crawler.toggled.connect(self._action_crawler_toggled)
        self._ui.action_run_files.triggered.connect(self._process_runs)
        self._ui.action_run_preview.triggered.connect(self._process_runs_preview)
        self._ui.action_run_streaming.triggered.connect(self._process_runs_streaming)
        self._ui.action_kill_processing.triggered.connect(self._kill_processing)
        self._ui.action_remove_processing.triggered.connect(self._remove_processing)
        self._ui.action_view_hits.triggered.connect(self._view_hits)
        self._ui.action_view_stream.triggered.connect(self._view_stream)
        self._ui.action_sum_of_blanks.triggered.connect(self._view_sum_blanks)
        self._ui.action_sum_of_hits.triggered.connect(self._view_sum_hits)
        self._ui.action_peak_powder.triggered.connect(self._view_powder_hits)
        self._ui.action_peakogram.triggered.connect(self._view_peakogram)
        self._ui.action_hitrate.triggered.connect(self._view_hitrate)
        self._ui.action_cell_explorer.triggered.connect(self._cell_explorer)

        # File menu actions
        self._ui.menu_file_start_crawler.triggered.connect(
            self._action_start_crawler_triggered
        )

        # Cheetah menu actions
        self._ui.menu_cheetah_process_runs.triggered.connect(self._process_runs)
        self._ui.menu_cheetah_process_preview.triggered.connect(
            self._process_runs_preview
        )
        self._ui.menu_cheetah_process_streaming.triggered.connect(
            self._process_runs_streaming
        )
        self._ui.menu_cheetah_kill_processing.triggered.connect(self._kill_processing)
        self._ui.menu_cheetah_process_jungfrau_darks.triggered.connect(
            self._process_jungfrau_darks
        )
        self._ui.menu_cheetah_remove_processing.triggered.connect(
            self._remove_processing
        )
        self._ui.menu_cheetah_peakfinder_parameters.triggered.connect(
            self._edit_peakfinder_parameters
        )

        # Mask menu actions
        self._ui.menu_mask_maker.triggered.connect(self._open_maskmaker)
        self._ui.menu_mask_view.triggered.connect(self._view_mask)
        self._ui.menu_mask_psana.triggered.connect(self._psana_mask)

        # Analysis menu items
        self._ui.menu_analysis_hitrate.triggered.connect(self._view_hitrate)
        self._ui.menu_analysis_peakogram.triggered.connect(self._view_peakogram)

        # Powder menu actions
        self._ui.menu_powder_hits_sum.triggered.connect(self._view_sum_hits)
        self._ui.menu_powder_blanks_sum.triggered.connect(self._view_sum_blanks)
        self._ui.menu_powder_peaks_hits.triggered.connect(self._view_powder_hits)
        self._ui.menu_powder_peaks_blanks.triggered.connect(self._view_powder_blanks)

        # Indexing menu actions
        self._ui.menu_indexing_view_stream.triggered.connect(self._view_stream)
        self._ui.menu_indexing_cell_explorer.triggered.connect(self._cell_explorer)

        # Log menu actions
        self._ui.menu_log_batch.triggered.connect(self._view_batch_log)
        self._ui.menu_log_cheetah_status.triggered.connect(self._view_status_file)
        self._ui.menu_log_om.triggered.connect(self._view_om_log)
        self._ui.menu_log_crystfel.triggered.connect(self._view_crystfel_log)

        # Disable action commands until enabled
        self._action_commands: List[QtWidgets.QAction] = [
            self._ui.action_run_files,
            self._ui.action_run_preview,
            self._ui.action_kill_processing,
            self._ui.action_remove_processing,
            self._ui.action_crawler,
            self._ui.menu_file_start_crawler,
            self._ui.menu_cheetah_process_runs,
            self._ui.menu_cheetah_process_preview,
            self._ui.menu_cheetah_kill_processing,
            self._ui.menu_cheetah_remove_processing,
            self._ui.menu_cheetah_process_jungfrau_darks,
            self._ui.menu_cheetah_peakfinder_parameters,
        ]
        self._streaming_commands: List[QtWidgets.QAction] = [
            self._ui.action_run_streaming,
            self._ui.menu_cheetah_process_streaming,
        ]
        action: QtWidgets.QAction
        for action in self._action_commands:
            action.setEnabled(False)
        for action in self._streaming_commands:
            action.setEnabled(False)
        self._ui.menu_file_command.triggered.connect(self._enable_commands)

        if command:
            self._enable_commands()
            self._action_start_crawler_triggered()

        if self.experiment.get_detector() == "Jungfrau1M":
            self._ui.menu_cheetah_process_jungfrau_darks.setVisible(True)

        if self.experiment.get_facility() == "LCLS":
            self._ui.menu_mask_psana.setVisible(True)
            self._ui.menu_mask_psana.setEnabled(True)

    def _crawler_gui_closed(self) -> None:
        # Prints a message when Crawler GUI is closed.
        self._ui.action_crawler.setChecked(False)
        logger.info("Crawler closed.")

    def _start_crawler(self) -> None:
        # Starts new Crawler GUI.
        logger.info("Starting crawler")
        self.crawler_window = CrawlerGui(self.experiment, self)
        self.crawler_window.scan_finished.connect(self._refresh_table)
        self.crawler_window.show()

    def _stop_crawler(self) -> None:
        # Stops Crawler GUI.
        if self.crawler_window is not None:
            self.crawler_window.close()

    def _action_crawler_toggled(self, checked: bool) -> None:
        # Starts or stops Crawler GUI.
        if checked:
            self._start_crawler()
        else:
            self._stop_crawler()

    def _action_start_crawler_triggered(self) -> None:
        # Triggers the action_crawler action.
        self._ui.action_crawler.setChecked(True)

    def _enable_commands(self) -> None:
        # Enables "command operations": starting the crawler and processing runs.
        action: QtWidgets.QAction
        for action in self._action_commands:
            action.setEnabled(True)
        if self.experiment._streaming_process is not None:
            for action in self._streaming_commands:
                action.setEnabled(True)

    def _edit_peakfinder_parameters(self) -> None:
        # Opens a dialog to select a config file and edit peakfinder parameters.
        selected_proc_dirs: Tuple[str, ...] = self._get_selected_rows().proc_dirs
        if len(selected_proc_dirs) == 0 or selected_proc_dirs[0] in ("---", ""):
            first_selected_hdf5_dir: Optional[str] = None
        else:
            first_selected_hdf5_dir = selected_proc_dirs[0]
        latest_config_template: str = self.experiment.get_last_processing_config(
            first_selected_hdf5_dir
        )["config_template"]
        selected_config_template: str = QtWidgets.QFileDialog().getOpenFileName(
            self, "Select config template file", latest_config_template, filter="*.yaml"
        )[0]
        if selected_config_template:
            process_dialogs.PeakfinderParametersDialog(
                selected_config_template, self
            ).exec_()

    def _exit(self) -> None:
        # Prints a message on exit
        print("Bye bye.")
        sys.exit(0)

    def _get_cwd(self) -> pathlib.Path:
        # Hack to get current directory without resolving links at psana
        # instead of using pathlib.Path.cwd()
        return pathlib.Path(os.environ["PWD"])

    def _get_selected_rows(
        self,
    ) -> _SelectedRows:
        # Get selected rows, run IDs and proc directories
        if len(self._tree.selectionModel().selectedRows()) == 0:
            return _SelectedRows(tuple(), tuple(), tuple(), tuple())

        selected: List[Tuple[float, str, str, QtCore.QModelIndex]] = []
        index: QtCore.QModelIndex
        for index in self._tree.selectionModel().selectedRows():
            run_id: str = self._data_model.data(index)
            row: float = float(index.row())
            if run_id == "":
                parent: QtCore.QModelIndex = index.parent()
                run_id = self._data_model.data(parent)
                row = index.parent().row() + (index.row() + 1) / 1000
            proc_dir: str = self._data_model.data(
                index.siblingAtColumn(self._proc_dir_column)
            )
            selected.append((row, run_id, proc_dir, index))

        return _SelectedRows(*zip(*sorted(selected, key=itemgetter(0))))  # type: ignore

    def _get_hit_files_from_selected(
        self, selected: _SelectedRows
    ) -> Dict[str, pathlib.Path]:
        # Get hit list files for selected runs
        hit_files: Dict[str, pathlib.Path] = {}
        for table_id, proc_dir in zip(selected.runs, selected.proc_dirs):
            run_id: str = self.experiment.crawler_table_id_to_raw_id(table_id)
            if proc_dir in ("---", ""):
                continue
            if run_id in hit_files:
                continue
            hits_file: Optional[pathlib.Path] = self.experiment.get_hits_filename(
                proc_dir
            )
            if hits_file is not None:
                hit_files[run_id] = hits_file

        return hit_files

    def _process_jungfrau_darks(self) -> None:
        # Process selected Jungfrau 1M dark runs
        selected: _SelectedRows = self._get_selected_rows()
        selected_runs: Set[str] = set(selected.runs)
        if len(selected_runs) == 0:
            return
        input_str: str = " ".join(selected_runs)
        latest_config: str = self.experiment.get_last_processing_config()[
            "config_template"
        ]
        selected_config: str = QtWidgets.QFileDialog().getOpenFileName(
            self, "Select config template file", latest_config, filter="*.yaml"
        )[0]
        if not selected_config:
            return
        crawler_config: pathlib.Path = (
            self.experiment.get_working_directory() / "gui/crawler.config"
        )
        process_darks_script: pathlib.Path = (
            self.experiment.get_calib_directory() / "process_darks_jungfrau.py"
        )
        process_darks_command: str = (
            f"{process_darks_script} {input_str} -e {crawler_config} "
            f"-c {selected_config} --copy-templates"
        )
        logger.info(f"Running command: {process_darks_command}")
        LoggingPopen(
            logger.getChild("process_jungfrau_darks"), process_darks_command, shell=True
        )
        row: float
        for row in selected.rows:
            self._data_model.item(int(row), self._cheetah_status_column).setText(
                "Started"
            )

    def _get_psana_detector_name(self) -> str:
        # Get psana detector name from OM config template
        config_template: pathlib.Path = pathlib.Path(
            self.experiment.get_last_processing_config()["config_template"]
        )
        if not config_template.exists():
            logger.error(
                f"Could not find processing config template, file {config_template} "
                f"doesn't exist."
            )
            return ""
        fh: TextIO
        detector_name: str = ""
        with open(config_template) as fh:
            for line in fh:
                if "psana_detector_name" in line:
                    detector_name = line.split(":")[-1].strip()
        return detector_name

    def _psana_mask(self) -> None:
        # Extract mask from psana and save it to file
        selected: _SelectedRows = self._get_selected_rows()
        if len(selected.runs) == 0:
            return
        selected_run: int = int(selected.runs[0])

        calib_directory: pathlib.Path = self.experiment.get_calib_directory()
        raw_directory: pathlib.Path = self.experiment.get_raw_directory()
        experiment_id: str = self.experiment.get_id()
        psana_detector_name: str = self._get_psana_detector_name()
        if not psana_detector_name:
            logger.error(
                "Could not extract psana detector name from the processing config "
                "template."
            )
            return

        psana_mask_script: pathlib.Path = calib_directory / "psana_mask.py"
        if not psana_mask_script.exists():
            logger.error(
                "Could not find psana_mask.py script in cheetah/calib directory."
            )
            return

        psana_source: str = (
            f"exp={experiment_id}:run={selected_run}:dir={raw_directory}"
        )
        suggested_filename: str = str(calib_directory / f"mask-r{selected_run:04d}.h5")
        output_filename: str = QtWidgets.QFileDialog().getSaveFileName(
            self, "Select output mask file", suggested_filename, filter="*.h5"
        )[0]
        if not output_filename:
            return

        command: str = (
            f"{psana_mask_script} -s {psana_source} -d {psana_detector_name} "
            f"-o {output_filename}"
        )
        logger.info(f"Running command: {command}")
        LoggingPopen(logger.getChild("psana_mask"), command, shell=True)

    def _kill_processing(self) -> None:
        # Ask if the user is sure they want to kill the jobs. If yes - try to kill the
        # jobs.
        selected: _SelectedRows = self._get_selected_rows()
        selected_proc_dirs: List[str] = []
        selected_indices: List[QtCore.QModelIndex] = []
        i: int
        for i in range(len(selected.proc_dirs)):
            if selected.proc_dirs[i] not in ("---", ""):
                selected_proc_dirs.append(selected.proc_dirs[i])
                selected_indices.append(selected.indices[i])
        if len(selected_proc_dirs) == 0:
            return

        reply: Any = QtWidgets.QMessageBox.question(
            self,
            "",
            "Are you sure you want to cancel processing of the selected runs?",
            QtWidgets.QMessageBox.Yes,
            QtWidgets.QMessageBox.No,
        )
        if reply == QtWidgets.QMessageBox.No:
            return

        self.experiment.kill_processing_jobs(selected_proc_dirs)
        for index in selected_indices:
            try:
                self._data_model.itemFromIndex(
                    index.siblingAtColumn(self._cheetah_status_column)
                ).setText("Cancelling")
            except AttributeError:
                pass

    def _remove_processing(self) -> None:
        # Ask if the user is sure they want to remove the processing results. If yes -
        # remove selected directories.
        selected: _SelectedRows = self._get_selected_rows()
        selected_proc_dirs: List[str] = []
        selected_indices: List[QtCore.QModelIndex] = []
        i: int
        for i in range(len(selected.proc_dirs)):
            if selected.proc_dirs[i] not in ("---", ""):
                selected_proc_dirs.append(selected.proc_dirs[i])
                selected_indices.append(selected.indices[i])
        if len(selected_proc_dirs) == 0:
            return

        message_dirs: str = "\n".join(selected_proc_dirs)
        reply: Any = QtWidgets.QMessageBox.question(
            self,
            "",
            f"Are you sure you want to remove all processing results of the selected "
            f"runs?\n\n"
            f"The following directories will be removed:\n{message_dirs}",
            QtWidgets.QMessageBox.Yes,
            QtWidgets.QMessageBox.No,
        )
        if reply == QtWidgets.QMessageBox.No:
            return

        self.experiment.remove_processing_results(selected_proc_dirs)
        for index in selected_indices:
            try:
                self._data_model.itemFromIndex(
                    index.siblingAtColumn(self._cheetah_status_column)
                ).setText("Removing")
            except AttributeError:
                pass

    def _process_runs(self, streaming: bool = False, save_data: bool = True) -> None:
        # Starts a ProcessThread which submits processing of selected runs
        selected: _SelectedRows = self._get_selected_rows()
        selected_runs: List[str] = [
            self.experiment.crawler_table_id_to_raw_id(run_id)
            for run_id in set(selected.runs)
        ]
        if len(selected_runs) == 0:
            return

        hit_files: Dict[str, pathlib.Path] = self._get_hit_files_from_selected(selected)
        if len(hit_files) == len(selected_runs):
            process_hits_option: bool = True
        else:
            process_hits_option = False

        first_selected_hdf5_dir: Optional[str] = selected.proc_dirs[0]
        if first_selected_hdf5_dir in ("", "---"):
            first_selected_hdf5_dir = None
        dialog: process_dialogs.RunProcessingDialog = (
            process_dialogs.RunProcessingDialog(
                self.experiment.get_last_processing_config(first_selected_hdf5_dir),
                streaming,
                process_hits_option,
                self,
            )
        )
        if dialog.exec() == 0:
            return

        processing_config: TypeProcessingConfig = dialog.get_config()
        process_hits: bool = dialog.process_hits()

        if process_hits:
            exp_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
            msg: str = "The hits from following files will be processed:\n" + "\n".join(
                f"Run {run_id}: {hit_file.relative_to(exp_proc_dir)}"
                for run_id, hit_file in hit_files.items()
            )
            reply: Any = QtWidgets.QMessageBox.information(
                self,
                "",
                msg,
                QtWidgets.QMessageBox.Ok,
                QtWidgets.QMessageBox.Cancel,
            )
            if reply == QtWidgets.QMessageBox.Cancel:
                return

        if not save_data:
            processing_config["write_data_files"] = False

        self._process_thread: ProcessThread = ProcessThread(
            self.experiment,
            selected_runs,
            processing_config,
            streaming,
            hit_files if process_hits else None,
        )
        self._process_thread.started.connect(self._process_thread_started)
        self._process_thread.finished.connect(self._process_thread_finished)
        self._process_thread.finished.connect(self._process_thread.deleteLater)
        self._process_thread.start()

        tag: str = processing_config["tag"]
        selected_rows: Set[int] = set((int(row) for row in selected.rows))
        self._update_rows_with_tag(tag, selected_rows)

    def _update_rows_with_tag(self, tag: str, selected_rows: Set[int]) -> None:
        row: int
        for row in selected_rows:
            index: QtCore.QModelIndex = self._data_model.index(row, 0)
            index_to_change: QtCore.QModelIndex = index
            change_existing_row: bool = False
            if (
                self._data_model.data(index.siblingAtColumn(self._dataset_tag_column))
                == tag
                or self._data_model.data(index.siblingAtColumn(self._proc_dir_column))
                == "---"
            ):
                change_existing_row = True
            elif self._data_model.hasChildren(index):
                child_row: int
                for child_row in range(self._data_model.rowCount(index)):
                    if (
                        self._data_model.data(
                            index.child(child_row, self._dataset_tag_column),
                        )
                        == tag
                    ):
                        change_existing_row = True
                        index_to_change = index.child(child_row, 0)
                        break
            if not change_existing_row:
                self._data_model.itemFromIndex(index).insertRow(
                    0, [QtGui.QStandardItem("") for _ in range(self._num_columns)]
                )
                index_to_change = index.child(0, 0)

            self._data_model.itemFromIndex(
                index_to_change.siblingAtColumn(self._dataset_tag_column),
            ).setText(tag)

            self._data_model.itemFromIndex(
                index_to_change.siblingAtColumn(self._cheetah_status_column),
            ).setText("Submitting")

    def _process_runs_preview(self) -> None:
        self._process_runs(save_data=False)

    def _process_runs_streaming(self) -> None:
        self._process_runs(streaming=True)

    def _process_thread_started(self) -> None:
        # Disables launching new processing jobs until the previous jobs are submitted
        self._ui.action_run_files.setEnabled(False)
        self._ui.action_run_preview.setEnabled(False)
        self._ui.action_kill_processing.setEnabled(False)
        self._ui.menu_cheetah_process_runs.setEnabled(False)
        self._ui.menu_cheetah_process_preview.setEnabled(False)
        self._ui.menu_cheetah_kill_processing.setEnabled(False)
        self._ui.action_run_streaming.setEnabled(False)
        self._ui.menu_cheetah_process_streaming.setEnabled(False)

    def _process_thread_finished(self) -> None:
        # Enables launching new processing jobs
        self._ui.action_run_files.setEnabled(True)
        self._ui.action_run_preview.setEnabled(True)
        self._ui.action_kill_processing.setEnabled(True)
        self._ui.menu_cheetah_process_runs.setEnabled(True)
        self._ui.menu_cheetah_process_preview.setEnabled(True)
        self._ui.menu_cheetah_kill_processing.setEnabled(True)
        if self.experiment._streaming_process is not None:
            self._ui.action_run_streaming.setEnabled(True)
            self._ui.menu_cheetah_process_streaming.setEnabled(True)

    def _refresh_table(self) -> None:
        # Refreshes runs table. This function is run automatically every minute. It can
        # also be run manually by clicking "Refresh table" button.
        self._refresh_timer.stop()
        if not self._crawler_csv_filename.exists():
            self._refresh_timer.start(60000)
            return

        # Remember selected rows
        selected_rows_run: List[str] = []
        selected_rows_proc: List[str] = []
        index: QtCore.QModelIndex
        for index in self._tree.selectionModel().selectedRows():
            if self._data_model.data(index) != "":
                selected_rows_run.append(self._data_model.data(index))
            else:
                selected_rows_proc.append(
                    self._data_model.data(index.siblingAtColumn(self._proc_dir_column))
                )

        # Remember collapsed rows
        collapsed_rows: List[str] = []
        row: int
        for row in range(self._data_model.rowCount()):
            index = self._data_model.index(row, 0)
            if self._data_model.hasChildren(index) and not self._tree.isExpanded(index):
                collapsed_rows.append(self._data_model.data(index))

        # Remember scroll position
        index = self._tree.indexAt(QtCore.QPoint(0, 0))
        if self._data_model.data(index) != "":
            scroll_run: str = self._data_model.data(index)
        else:
            scroll_run = self._data_model.data(index.parent())

        # Read data from crawler CSV file
        fh: TextIO
        with open(self._crawler_csv_filename, "r") as fh:
            self._table_data = list(csv.DictReader(fh))

        # Update table
        self._data_model.setRowCount(0)
        root: Any = self._data_model.invisibleRootItem()
        data: Dict[str, Any]
        scroll_index: Optional[QtCore.QModelIndex] = None
        for data in self._table_data:
            if data["Run"] != "":
                parent: Any = root
            else:
                parent = root.child(root.rowCount() - 1)
            tree_row: List[Any] = []
            key: str
            for key in self._table_column_names:
                item = QtGui.QStandardItem()
                if key in data.keys():
                    if key in ("Nprocessed", "Nindexed", "Hitrate", "Idxrate"):
                        # Try converting and displaying data as float
                        try:
                            item.setData(float(data[key]), role=QtCore.Qt.DisplayRole)
                        except ValueError:
                            item.setText(data[key])
                    else:
                        # All other data is displayed as text
                        item.setText(data[key])
                tree_row.append(item)
            parent.appendRow(tree_row)

            # Restore previous selection
            if (
                data["Run"] in selected_rows_run
                or data["H5Directory"] in selected_rows_proc
            ):
                self._tree.selectionModel().select(
                    self._data_model.indexFromItem(tree_row[0]),
                    QtCore.QItemSelectionModel.Select | QtCore.QItemSelectionModel.Rows,
                )

            # Find index of the row to scroll to
            if data["Run"] == scroll_run:
                scroll_index = self._data_model.indexFromItem(tree_row[0])

        self._tree.expandAll()

        # Collapse previously collapsed rows
        for row in range(self._data_model.rowCount()):
            index = self._data_model.index(row, 0)
            if self._data_model.data(index) in collapsed_rows:
                self._tree.setExpanded(index, False)

        # Scroll to the previous position
        if scroll_index is not None:
            self._tree.scrollTo(scroll_index, self._tree.PositionAtTop)

        logger.info("Table refreshed.")
        self._refresh_timer.start(60000)

    def _select_experiment(self) -> None:
        # Creates self.experiment - an instance of CheetahExperiment class - either by
        # loading already existing Cheetah experiment from disk or creating a new one.
        # Opens experiment selection dialog if current working directory doesn't have
        # crawler.config file.
        if pathlib.Path("./crawler.config").is_file():
            working_directory: pathlib.Path = self._get_cwd()
        else:
            dialog: setup_dialogs.ExperimentSelectionDialog = (
                setup_dialogs.ExperimentSelectionDialog(self)
            )
            if dialog.exec() == 0:
                print("Catch you another time.")
                self._exit()
            working_directory = dialog.get_experiment()

        if (working_directory / "crawler.config").is_file():
            self.experiment: CheetahExperiment = CheetahExperiment(
                path=working_directory
            )
        else:
            self._setup_new_experiment(working_directory)

    def _setup_new_experiment(self, path: pathlib.Path) -> None:
        # Sets up new Cheetah experiment.
        logger.info(f"Selected directory: {path}")
        dialog: setup_dialogs.SetupNewExperimentDialog = (
            setup_dialogs.SetupNewExperimentDialog(path, self)
        )
        if dialog.exec() == 0:
            self._select_experiment()
        else:
            new_experiment_config: ExperimentConfig = dialog.get_config()
            self.experiment = CheetahExperiment(
                path, new_experiment_config=new_experiment_config
            )

    def _view_batch_log(self) -> None:
        # Shows the contents of batch.out file from the first of the selected runs in
        # a TextFileGui window.
        selected_proc_dirs: Tuple[str, ...] = self._get_selected_rows().proc_dirs
        if len(selected_proc_dirs) == 0 or selected_proc_dirs[0] in ("---", ""):
            return
        batch_file: pathlib.Path = (
            self.experiment.get_proc_directory() / selected_proc_dirs[0] / "batch.out"
        )
        if not batch_file.exists():
            logger.error(f"Batch log file {batch_file} doesn't exist.")
        else:
            TextFileViewer(str(batch_file), self)

    def _view_om_log(self) -> None:
        # Shows the contents of om.out file from the first of the selected runs in
        # a TextFileGui window.
        selected_proc_dirs: Tuple[str, ...] = self._get_selected_rows().proc_dirs
        if len(selected_proc_dirs) == 0 or selected_proc_dirs[0] in ("---", ""):
            return
        batch_file: pathlib.Path = (
            self.experiment.get_proc_directory() / selected_proc_dirs[0] / "om.out"
        )
        if not batch_file.exists():
            logger.error(f"OM log file {batch_file} doesn't exist.")
        else:
            TextFileViewer(str(batch_file), self)

    def _view_crystfel_log(self) -> None:
        # Shows the contents of crystfel.out file from the first of the selected runs in
        # a TextFileGui window.
        selected_proc_dirs: Tuple[str, ...] = self._get_selected_rows().proc_dirs
        if len(selected_proc_dirs) == 0 or selected_proc_dirs[0] in ("---", ""):
            return
        batch_file: pathlib.Path = (
            self.experiment.get_proc_directory()
            / selected_proc_dirs[0]
            / "crystfel.out"
        )
        if not batch_file.exists():
            logger.error(f"CrystFEL log file {batch_file} doesn't exist.")
        else:
            TextFileViewer(str(batch_file), self)

    def _view_status_file(self) -> None:
        # Shows the contents of status.txt file from the first of the selected runs in
        # a TextFileGui window.
        selected_proc_dirs: Tuple[str, ...] = self._get_selected_rows().proc_dirs
        if len(selected_proc_dirs) == 0 or selected_proc_dirs[0] in ("---", ""):
            return
        status_file: pathlib.Path = (
            self.experiment.get_proc_directory() / selected_proc_dirs[0] / "status.txt"
        )
        if not status_file.exists():
            logger.error(f"Status file {status_file} doesn't exist.")
        else:
            TextFileViewer(str(status_file), self)

    def _get_geometry_and_mask_arg_for_viewer(
        self, proc_dir: Union[str, pathlib.Path]
    ) -> str:
        # Gets geometry and mask file arguments for the viewer from the monitor.yaml
        # file in the proc directory.
        om_config_file: pathlib.Path = pathlib.Path(proc_dir) / "monitor.yaml"
        if om_config_file.exists():
            monitor_parameters: Dict[str, Any] = load_configuration_parameters(
                config=Path(om_config_file)
            )

            try:
                parameters: _MonitorParameters = _MonitorParameters.model_validate(
                    monitor_parameters
                )
            except ValidationError as exception:
                raise OmConfigurationFileSyntaxError(
                    "Error parsing parameters for the Peakfinder8PeakDetection algorithm: "
                    f"{exception}"
                )

            geometry: str = parameters.crystallography.geometry_file
            mask: Optional[str] = (
                parameters.peakfinder8_peak_detection.bad_pixel_map_filename
            )
        else:
            geometry = self.experiment.get_last_processing_config()["geometry"]
            mask = None

        geometry_arg: str = f"-g {geometry}"
        mask_arg: str = f"-m {mask}" if mask is not None else ""
        return f"{geometry_arg} {mask_arg}"

    def _view_hits(self) -> None:
        # Launches Cheetah Viewer showing hits from selected runs.
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[str] = [
            str(root_proc_dir / proc_dir)
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        if len(selected_directories) == 0:
            return

        input_str: str = " ".join(selected_directories)
        args: str = self._get_geometry_and_mask_arg_for_viewer(selected_directories[0])
        viewer_command: str = f"cheetah_viewer.py {input_str} -i dir {args}"
        logger.info(f"Running command: {viewer_command}")
        LoggingPopen(logger.getChild("viewer"), viewer_command, shell=True)

    def _view_stream(self) -> None:
        # Launches Cheetah Viewer showing stream files from selected runs
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            root_proc_dir / proc_dir
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        stream_files: List[str] = []
        dir: pathlib.Path
        for dir in selected_directories:
            file: pathlib.Path
            for file in dir.glob("*.stream"):
                stream_files.append(str(file))
        if len(stream_files) == 0:
            logger.info("There's no stream files in the selected directories yet.")
            return
        input_str: str = " ".join(stream_files)
        viewer_command: str = f"cheetah_viewer.py -i stream {input_str}"
        logger.info(f"Running command: {viewer_command}")
        LoggingPopen(logger.getChild("viewer"), viewer_command, shell=True)

    def _view_hitrate(self) -> None:
        # Launches Cheetah Hitrate GUI for selected runs.
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            root_proc_dir / proc_dir
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        frame_files: List[str] = [
            str(dir / "frames.txt")
            for dir in selected_directories
            if (dir / "frames.txt").exists()
        ]
        if len(frame_files) == 0:
            logger.info("There's no frames.txt files in the selected directories yet.")
            return

        input_str: str = " ".join(frame_files)
        command: str = f"cheetah_hitrate.py {input_str}"
        logger.info(f"Running command: {command}")
        LoggingPopen(logger.getChild("hitrate"), command, shell=True)

    def _view_peakogram(self) -> None:
        # Launches Cheetah Peakogram GUI for selected runs.
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            root_proc_dir / proc_dir
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        peak_files: List[str] = [
            str(dir / "peaks.txt")
            for dir in selected_directories
            if (dir / "peaks.txt").exists()
        ]
        if len(peak_files) == 0:
            logger.info("There's no peaks.txt files in the selected directories yet.")
            return

        input_str: str = " ".join(peak_files)
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        command: str = f"cheetah_peakogram.py {input_str} -g {geometry}"
        logger.info(f"Running command: {command}")
        LoggingPopen(logger.getChild("peakogram"), command, shell=True)

    def _view_mask(self) -> None:
        # Open mask file selection dialog and launches Cheetah Viewer on the selected
        # file.
        latest_mask: str = self.experiment.get_last_processing_config()["mask"]
        if latest_mask != "" and pathlib.Path(latest_mask).is_file():
            path: str = latest_mask
        else:
            path = str(self.experiment.get_calib_directory())
        filename: str = QtWidgets.QFileDialog().getOpenFileName(
            self, "Select mask file", path, filter="*.h5"
        )[0]
        if not filename:
            return
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        viewer_command: str = (
            f"cheetah_viewer.py {filename} -d /data/data -g {geometry}"
        )
        logger.info(f"Running command: {viewer_command}")
        LoggingPopen(logger.getChild("viewer"), viewer_command, shell=True)

    def _view_powder_hits(self) -> None:
        # Launches Cheetah Viewer showing the hits peakpowder.
        self._view_sums(1, "/data/peakpowder")

    def _view_powder_blanks(self) -> None:
        # Launches Cheetah Viewer showing the blanks peakpowder.
        self._view_sums(0, "/data/peakpowder")

    def _view_sum_hits(self) -> None:
        # Launches Cheetah Viewer showing the sum of hits.
        self._view_sums(1, "/data/data")

    def _view_sum_blanks(self) -> None:
        # Launches Cheetah Viewer showing the sum of blanks.
        self._view_sums(0, "/data/data")

    def _open_maskmaker(self) -> None:
        # Launches Cheetah Viewer showing sum of hits with Maskmaker tab open.
        self._view_sums(1, "/data/data", maskmaker=True)

    def _view_sums(
        self,
        sum_class: Literal[0, 1],
        hdf5_dataset: Literal["/data/data", "/data/peakpowder"],
        maskmaker: bool = False,
    ) -> None:
        # Launches Cheetah Viewer showing requested sums from selected runs.
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            root_proc_dir / proc_dir
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        sum_files: List[str] = []
        dir: pathlib.Path
        for dir in selected_directories:
            file: pathlib.Path
            for file in dir.glob(f"*-class{sum_class}-sum.h5"):
                sum_files.append(str(file))
        if len(sum_files) == 0:
            logger.info(
                f"There's no class{sum_class} sum files in the selected directories yet."
            )
            return
        input_str: str = " ".join(sum_files)
        args: str = self._get_geometry_and_mask_arg_for_viewer(selected_directories[0])
        if maskmaker:
            args += " --maskmaker"

        viewer_command: str = f"cheetah_viewer.py {input_str} -d {hdf5_dataset} {args}"
        logger.info(f"Running command: {viewer_command}")
        LoggingPopen(logger.getChild("viewer"), viewer_command, shell=True)

    def _cell_explorer(self) -> None:
        # Launches CrystFEL cell_explorer with the first stream file found in the
        # selected runs.
        selected: _SelectedRows = self._get_selected_rows()
        root_proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            root_proc_dir / proc_dir
            for proc_dir in selected.proc_dirs
            if proc_dir not in ("---", "")
        ]
        stream_files: List[str] = []
        dir: pathlib.Path
        for dir in selected_directories:
            file: pathlib.Path
            for file in dir.glob("*.stream"):
                stream_files.append(str(file))
        if len(stream_files) == 0:
            logger.info("There's no stream files in the selected directories yet.")
            return
        command: str = f"cell_explorer {stream_files[0]} 2>&1"
        logger.info(f"Running command: {command}")
        LoggingPopen(logger.getChild("cell_explorer"), command, shell=True)

    def keyPressEvent(self, event):
        """
        Process key press events.
        """
        if event.key() == QtCore.Qt.Key_Delete:
            self._remove_processing()
        else:
            super().keyPressEvent(event)

    def closeEvent(self, event: Any) -> None:
        """
        Close all Cheetah GUI windows.

        This function is called when the main GUI is closed. It closes all child Viewer
        windows.
        """
        print("Bye bye.")
        event.accept()


@click.command(context_settings=dict(help_option_names=["-h", "--help"]))  # type: ignore
@click.option(  # type: ignore
    "--command", "-c", "command", is_flag=True, default=False, hidden=True
)
@click.option(  # type: ignore
    "--verbose",
    "-v",
    "verbose",
    is_flag=True,
    default=False,
    help="Print logging messages to console.",
)
def main(command: bool, verbose: bool) -> None:
    """
    Cheetah GUI. This script starts the main Cheetah window. If started from the
    existing Cheetah experiment directory containing crawler.config file experiment
    will be loaded automatically. Otherwise, a new experiment selection dialog will be
    opened.
    """
    # Create logging directory if it doesn't exist.
    (pathlib.Path.home() / ".cheetah/logs").mkdir(parents=True, exist_ok=True)

    # Set up logging.
    if verbose:
        logging_config["loggers"]["cheetah"]["handlers"].append("console_gui")
    logging.config.dictConfig(logging_config)

    app: Any = QtWidgets.QApplication(sys.argv)
    _ = CheetahGui(command)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

"""
Cheetah GUI.

This module contains the implementation of the main Cheetah GUI.
"""
import click  # type: ignore
import csv
import os
import pathlib
import subprocess
import sys
import yaml

from ansi2html import Ansi2HTMLConverter  # type: ignore
from datetime import datetime
from PyQt5 import QtGui, QtCore, QtWidgets, uic  # type: ignore
from typing import Any, List, Dict, Union, TextIO

try:
    from typing import Literal
except:
    from typing_extensions import Literal  # type: ignore

from cheetah.crawlers.base import Crawler, TypeTableRow
from cheetah.dialogs import setup_dialogs, process_dialogs
from cheetah import __file__ as cheetah_src_path
from cheetah.experiment import CheetahExperiment, TypeExperimentConfig
from cheetah.process import TypeProcessingConfig


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
        self._refresh_timer.start(60000)
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
    ) -> None:
        """
        Process Thread.

        This class is initialized when Cheetah processing is triggered for a list of
        runs from Cheetah GUI, creating a separate Qt thread. When the thread is
        started it calls the [run][cheetah.gui.ProcessThread.run] function, which
        launches processing for each run in the list calling.

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
        """
        super(ProcessThread, self).__init__()
        self._experiment: CheetahExperiment = experiment
        self._runs: List[str] = runs
        self._config: TypeProcessingConfig = config

    def run(self) -> None:
        """
        Process runs.

        This function is called when ProcessThread is started. It calls Cheetah
        Experiment [process_runs][cheetah.experiment.Experiment.process_runs] function.
        """
        self._experiment.process_runs(self._runs, self._config)


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

        self._select_experiment()
        self.setWindowTitle(f"Cheetah GUI: {self.experiment.get_working_directory()}")
        self._crawler_csv_filename: pathlib.Path = (
            self.experiment.get_crawler_csv_filename()
        )

        self._table: Any = self._ui.table_status
        self._table_data: List[Dict[str, Any]] = []
        self._table_column_names: List[str] = list(TypeTableRow.__annotations__.keys())
        self._table.setColumnCount(len(self._table_column_names))
        self._table.setHorizontalHeaderLabels(self._table_column_names)
        self._table.horizontalHeader().setDefaultSectionSize(
            self.width() // len(self._table_column_names)
        )
        self._table.setSortingEnabled(True)
        self._table.setEditTriggers(QtWidgets.QAbstractItemView.NoEditTriggers)
        self._table.horizontalHeader().setSectionResizeMode(
            QtWidgets.QHeaderView.Interactive
        )
        self._table.horizontalHeader().setSectionResizeMode(
            self._table.columnCount() - 1, QtWidgets.QHeaderView.Stretch
        )
        self._table.setWordWrap(False)
        self._table.verticalHeader().setVisible(False)

        # self._table.setSelectionMode(QtWidgets.QAbstractItemView.SingleSelection)
        self._table.setSelectionBehavior(QtWidgets.QAbstractItemView.SelectRows)

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
            "Cancelled": QtGui.QColor(255, 200, 200),
            "Error": QtGui.QColor(255, 100, 100),
        }

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh_table)

        self._refresh_table()

        # Connect front panel buttons to actions
        self._ui.button_refresh.clicked.connect(self._refresh_table)
        self._ui.button_run_cheetah.clicked.connect(self._process_runs)
        self._ui.button_kill_processing.clicked.connect(self._kill_processing)
        self._ui.button_view_hits.clicked.connect(self._view_hits)
        self._ui.button_sum_blanks.clicked.connect(self._view_sum_blanks)
        self._ui.button_sum_hits.clicked.connect(self._view_sum_hits)
        self._ui.button_peak_powder.clicked.connect(self._view_powder_hits)
        self._ui.button_peakogram.clicked.connect(self._view_peakogram)

        # File menu actions
        self._ui.menu_file_start_crawler.triggered.connect(self._start_crawler)

        # Cheetah menu actions
        self._ui.menu_cheetah_process_selected.triggered.connect(self._process_runs)
        self._ui.menu_cheetah_kill_processing.triggered.connect(self._kill_processing)
        self._ui.menu_cheetah_process_jungfrau_darks.triggered.connect(
            self._process_jungfrau_darks
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

        # Log menu actions
        self._ui.menu_log_batch.triggered.connect(self._view_batch_log)
        self._ui.menu_log_cheetah_status.triggered.connect(self._view_status_file)

        # Disable action commands until enabled
        self._ui.button_run_cheetah.setEnabled(False)
        self._ui.button_kill_processing.setEnabled(False)
        self._ui.menu_file_start_crawler.setEnabled(False)
        self._ui.menu_cheetah_process_selected.setEnabled(False)
        self._ui.menu_cheetah_kill_processing.setEnabled(False)
        self._ui.menu_cheetah_process_jungfrau_darks.setEnabled(False)
        self._ui.menu_file_command.triggered.connect(self._enable_commands)

        if command:
            self._enable_commands()
            self._start_crawler()

        if self.experiment.get_detector() == "Jungfrau1M":
            self._ui.menu_cheetah_process_jungfrau_darks.setVisible(True)

        if self.experiment.get_facility() == "LCLS":
            self._ui.menu_mask_psana.setVisible(True)

    def _crawler_gui_closed(self) -> None:
        # Prints a message when Crawler GUI is closed.
        print("Crawler closed.")

    def _enable_commands(self) -> None:
        # Enables "command operations": starting the crawler and processing runs.
        self._ui.button_run_cheetah.setEnabled(True)
        self._ui.button_kill_processing.setEnabled(True)
        self._ui.menu_file_start_crawler.setEnabled(True)
        self._ui.menu_cheetah_process_selected.setEnabled(True)
        self._ui.menu_cheetah_kill_processing.setEnabled(True)
        self._ui.menu_cheetah_process_jungfrau_darks.setEnabled(True)

    def _exit(self) -> None:
        # Prints message on exit
        print("Bye bye.")
        sys.exit(0)

    def _get_cwd(self) -> pathlib.Path:
        # Hack to get current directory without resolving links at psana
        # instead of using pathlib.Path.cwd()
        return pathlib.Path(os.environ["PWD"])

    def _process_jungfrau_darks(self) -> None:
        # Process selected Jungfrau 1M dark runs
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        selected_runs: List[str] = [
            self._table.item(row, 0).text() for row in selected_rows
        ]
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
        print(process_darks_command)
        subprocess.Popen(process_darks_command, shell=True)
        cheetah_column: int = self._table_column_names.index("Cheetah")
        row: int
        for row in selected_rows:
            self._table.setItem(
                row, cheetah_column, QtWidgets.QTableWidgetItem("Started")
            )
            self._table.item(row, cheetah_column).setBackground(
                self._status_colors["Started"]
            )

    def _get_psana_detector_name(self) -> str:
        # Get psana detector name from OM config template
        config_template: pathlib.Path = pathlib.Path(
            self.experiment.get_last_processing_config()["config_template"]
        )
        if not config_template.exists():
            print(
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
        if len(self._table.selectionModel().selectedRows()) == 0:
            return
        selected_run: int = sorted(
            (
                int(self._table.item(index.row(), 0).text())
                for index in self._table.selectionModel().selectedRows()
            )
        )[0]

        calib_directory: pathlib.Path = self.experiment.get_calib_directory()
        raw_directory: pathlib.Path = self.experiment.get_raw_directory()
        experiment_id: str = self.experiment.get_id()
        psana_detector_name: str = self._get_psana_detector_name()
        if not psana_detector_name:
            print(
                "Could not extract psana detector name from the processing config "
                "template."
            )
            return

        psana_mask_script: pathlib.Path = calib_directory / "psana_mask.py"
        if not psana_mask_script.exists():
            print("Could not find psana_mask.py script in cheetah/calib directory.")
            return

        psana_source: str = (
            f"exp={experiment_id}:run={selected_run}:dir={raw_directory}"
        )
        suggested_filename: str = str(calib_directory / f"mask-r{selected_run}:04d.h5")
        output_filename: str = QtWidgets.QFileDialog().getSaveFileName(
            self, "Select output mask file", suggested_filename, filter="*.h5"
        )[0]
        if not output_filename:
            return

        command: str = (
            f"{psana_mask_script} -s {psana_source} -d {psana_detector_name} "
            f"-o {output_filename}"
        )
        print(command)
        subprocess.Popen(command, shell=True)

    def _kill_processing(self) -> None:
        # Ask if the user is sure they want to kill the jobs. If yes - try to kill the
        # jobs.
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        if len(selected_rows) == 0:
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

        proc_dir_column: int = self._table_column_names.index("H5Directory")
        selected_run_dirs: List[str] = [
            (self._table.item(row, proc_dir_column).text())
            for row in selected_rows
            if self._table.item(row, proc_dir_column).text() != "---"
        ]
        self.experiment.kill_processing_jobs(selected_run_dirs)

    def _process_runs(self) -> None:
        # Starts a ProcessThread which submits processing of selected runs
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        selected_runs: List[str] = [
            self.experiment.crawler_table_id_to_raw_id(self._table.item(row, 0).text())
            for row in selected_rows
        ]
        if len(selected_runs) == 0:
            return
        first_selected_hdf5_dir: Union[str, None] = self._table.item(
            selected_rows[0], self._table_column_names.index("H5Directory")
        ).text()
        if first_selected_hdf5_dir == "---":
            first_selected_hdf5_dir = None

        dialog: process_dialogs.RunProcessingDialog = (
            process_dialogs.RunProcessingDialog(
                self.experiment.get_last_processing_config(first_selected_hdf5_dir),
                self,
            )
        )
        if dialog.exec() == 0:
            return
        else:
            processing_config: TypeProcessingConfig = dialog.get_config()
            self._process_thread: ProcessThread = ProcessThread(
                self.experiment,
                selected_runs,
                processing_config,
            )
            self._process_thread.started.connect(self._process_thread_started)
            self._process_thread.finished.connect(self._process_thread_finished)
            self._process_thread.finished.connect(self._process_thread.deleteLater)
            self._process_thread.start()

            cheetah_column: int = self._table_column_names.index("Cheetah")
            row: int
            for row in selected_rows:
                self._table.setItem(
                    row, cheetah_column, QtWidgets.QTableWidgetItem("Submitting")
                )
                self._table.item(row, cheetah_column).setBackground(
                    self._status_colors["Submitting"]
                )

    def _process_thread_started(self) -> None:
        # Disables launching new processing jobs until the previous jobs are submitted
        self._ui.button_run_cheetah.setEnabled(False)
        self._ui.button_kill_processing.setEnabled(False)
        self._ui.menu_cheetah_process_selected.setEnabled(False)
        self._ui.menu_cheetah_kill_processing.setEnabled(False)

    def _process_thread_finished(self) -> None:
        # Enables launching new processing jobs
        self._ui.button_run_cheetah.setEnabled(True)
        self._ui.button_kill_processing.setEnabled(True)
        self._ui.menu_cheetah_process_selected.setEnabled(True)
        self._ui.menu_cheetah_kill_processing.setEnabled(True)

    def _refresh_table(self) -> None:
        # Refreshes runs table. This function is run automatically every minute. It can
        # also be run manually by clicking "Refresh table" button.
        self._refresh_timer.stop()
        if not self._crawler_csv_filename.exists():
            self._refresh_timer.start(60000)
            return
        self._table.setSortingEnabled(False)
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        n_columns: int = len(self._table_column_names)
        fh: TextIO
        with open(self._crawler_csv_filename, "r") as fh:
            self._table_data = list(csv.DictReader(fh))

        if len(self._table_data) == 0:
            return
        n_rows: int = len(self._table_data)
        self._table.setRowCount(n_rows)
        self._table.setColumnCount(n_columns)
        self._table.updateGeometry()

        column: int
        row: int
        name: str
        data: Dict[str, Any]
        for column, name in enumerate(self._table_column_names):
            if name in self._table_data[0].keys():
                for row, data in enumerate(self._table_data):
                    value: str = data[name]
                    item: Any = QtWidgets.QTableWidgetItem()
                    try:
                        item.setData(QtCore.Qt.DisplayRole, float(value))
                    except ValueError:
                        item.setText(value)
                    self._table.setItem(row, column, item)

                    self._table.item(row, column).setBackground(
                        QtGui.QColor(255, 255, 255)
                    )
                    if name in ("Rawdata", "Cheetah"):
                        if value in self._status_colors.keys():
                            self._table.item(row, column).setBackground(
                                self._status_colors[value]
                            )

        self._table.resizeRowsToContents()
        self._table.setSortingEnabled(True)

        # restore previous selection:
        self._table.clearSelection()
        self._table.setSelectionMode(QtWidgets.QAbstractItemView.MultiSelection)
        for row in selected_rows:
            self._table.selectRow(row)
        self._table.setSelectionMode(QtWidgets.QAbstractItemView.ExtendedSelection)

        print(f"Table refreshed at {datetime.now()}")

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
        print(path)
        dialog: setup_dialogs.SetupNewExperimentDialog = (
            setup_dialogs.SetupNewExperimentDialog(path, self)
        )
        if dialog.exec() == 0:
            self._select_experiment()
        else:
            new_experiment_config: TypeExperimentConfig = dialog.get_config()
            self.experiment = CheetahExperiment(
                path, new_experiment_config=new_experiment_config
            )

    def _start_crawler(self) -> None:
        # Starts new Crawler GUI.
        print("Starting crawler")
        self.crawler_window = CrawlerGui(self.experiment, self)
        self.crawler_window.scan_finished.connect(self._refresh_table)
        self.crawler_window.show()

    def _view_batch_log(self) -> None:
        # Shows the contents of batch.out file from the first of the selected runs in
        # a TextFileGui window.
        if len(self._table.selectionModel().selectedRows()) == 0:
            return
        selected_row: int = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )[0]
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        batch_file: pathlib.Path = (
            self.experiment.get_proc_directory()
            / self._table.item(selected_row, proc_dir_column).text()
            / "batch.out"
        )
        if not batch_file.exists():
            print(f"Batch log file {batch_file} doesn't exist.")
        else:
            TextFileViewer(str(batch_file), self)

    def _view_status_file(self) -> None:
        # Shows the contents of status.txt file from the first of the selected runs in
        # a TextFileGui window.
        if len(self._table.selectionModel().selectedRows()) == 0:
            return
        selected_row: int = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )[0]
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        status_file: pathlib.Path = (
            self.experiment.get_proc_directory()
            / self._table.item(selected_row, proc_dir_column).text()
            / "status.txt"
        )
        if not status_file.exists():
            print(f"Status file {status_file} doesn't exist.")
        else:
            TextFileViewer(str(status_file), self)

    def _view_hits(self) -> None:
        # Launches Cheetah Viewer showing hits from selected runs.
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[str] = [
            str(proc_dir / self._table.item(row, proc_dir_column).text())
            for row in selected_rows
            if self._table.item(row, proc_dir_column).text() != "---"
        ]
        if len(selected_directories) == 0:
            return
        input_str: str = " ".join(selected_directories)
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        viewer_command: str = f"cheetah_viewer.py {input_str} -i dir -g {geometry}"
        print(viewer_command)
        p: subprocess.Popen[bytes] = subprocess.Popen(viewer_command, shell=True)

    def _view_hitrate(self) -> None:
        # Launches Cheetah Hitrate GUI for selected runs.
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        row: int
        frames_files: List[str] = []
        for row in selected_rows:
            filename: pathlib.Path = (
                proc_dir / self._table.item(row, proc_dir_column).text() / "frames.txt"
            )
            if filename.exists():
                frames_files.append(str(filename))

        if len(frames_files) == 0:
            print("There's no frames.txt files in the selected directories yet.")
            return

        input_str: str = " ".join(frames_files)
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        peakogram_gui_command: str = f"cheetah_hitrate.py {input_str}"
        print(peakogram_gui_command)
        subprocess.Popen(peakogram_gui_command, shell=True)

    def _view_peakogram(self) -> None:
        # Launches Cheetah Peakogram GUI for selected runs.
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        row: int
        peak_files: List[str] = []
        for row in selected_rows:
            filename: pathlib.Path = (
                proc_dir / self._table.item(row, proc_dir_column).text() / "peaks.txt"
            )
            if filename.exists():
                peak_files.append(str(filename))

        if len(peak_files) == 0:
            print("There's no peaks.txt files in the selected directories yet.")
            return

        input_str: str = " ".join(peak_files)
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        peakogram_gui_command: str = f"cheetah_peakogram.py {input_str} -g {geometry}"
        print(peakogram_gui_command)
        subprocess.Popen(peakogram_gui_command, shell=True)

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
        print(viewer_command)
        subprocess.Popen(viewer_command, shell=True)

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
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        proc_dir_column: int = self._table_column_names.index("H5Directory")
        proc_dir: pathlib.Path = self.experiment.get_proc_directory()
        selected_directories: List[pathlib.Path] = [
            proc_dir / self._table.item(row, proc_dir_column).text()
            for row in selected_rows
        ]
        sum_files: List[str] = []
        dir: pathlib.Path
        for dir in selected_directories:
            file: pathlib.Path
            for file in dir.glob(f"*-class{sum_class}-sum.h5"):
                sum_files.append(str(file))
        if len(sum_files) == 0:
            print(
                f"There's no class{sum_class} sum files in the selected directories yet."
            )
            return
        input_str: str = " ".join(sum_files)
        geometry: str = self.experiment.get_last_processing_config()["geometry"]
        if maskmaker:
            extra: str = "--maskmaker"
        else:
            extra = ""
        viewer_command: str = (
            f"cheetah_viewer.py {input_str} -d {hdf5_dataset} -g {geometry} {extra}"
        )
        print(viewer_command)
        subprocess.Popen(viewer_command, shell=True)

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
def main(command: bool) -> None:
    """
    Cheetah GUI. This script starts the main Cheetah window. If started from the
    existing Cheetah experiment directory containing crawler.config file experiment
    will be loaded automatically. Otherwise, a new experiment selection dialog will be
    opened.
    """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = CheetahGui(command)
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

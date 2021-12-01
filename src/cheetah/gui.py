import click  # type: ignore
import csv
import pathlib
import sys

from datetime import datetime
from PyQt5 import QtCore, QtWidgets, uic  # type: ignore
from typing import Any, List, Dict, TextIO

from cheetah.crawlers.base import Crawler
from cheetah.dialogs import setup_dialogs
from cheetah import __file__ as cheetah_src_path
from cheetah.experiment import CheetahExperiment, TypeExperimentConfig


class CrawlerRefresher(QtCore.QObject):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    finished = QtCore.pyqtSignal()

    def __init__(self, crawler: Crawler) -> None:
        """ """
        super(CrawlerRefresher, self).__init__()
        self._crawler: Crawler = crawler

    def refresh(self) -> None:
        self._crawler.update()
        self.finished.emit()


class CrawlerGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(self, experiment: CheetahExperiment, parent: Any = None) -> None:
        """ """
        super(CrawlerGui, self).__init__(parent)
        self.parent: Any = parent
        self.resize(300, 50)
        self.setWindowTitle("Cheetah Crawler")

        self._refresh_button: Any = QtWidgets.QPushButton("Refresh")
        self._refresh_button.clicked.connect(self._refresh)
        self._status_label: Any = QtWidgets.QLabel()

        layout: Any = QtWidgets.QHBoxLayout()
        layout.addWidget(self._refresh_button)
        layout.addWidget(self._status_label)
        self._central_widget = QtWidgets.QWidget()
        self._central_widget.setLayout(layout)
        self.setCentralWidget(self._central_widget)

        self._experiment: CheetahExperiment = experiment
        self._refresher = CrawlerRefresher(self._experiment.start_crawler())
        self._refresh_thread: Any = QtCore.QThread()
        self._refresher.moveToThread(self._refresh_thread)

        self._refresh_thread.started.connect(self._refresher.refresh)
        self._refresher.finished.connect(self._refresh_finished)
        self._refresher.finished.connect(self._refresh_thread.quit)

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh)

        self._refresh()

    def _refresh(self) -> None:
        self._refresh_button.setEnabled(False)
        self._status_label.setText("Scanning files")
        self._refresh_thread.start()

    def _refresh_finished(self) -> None:
        self._refresh_button.setEnabled(True)
        self._status_label.setText("Ready")
        self._refresh_timer.start(60000)

    def closeEvent(self, event: Any) -> None:
        self.parent._crawler_gui_closed()
        event.accept()


class CheetahGui(QtWidgets.QMainWindow):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    def __init__(self) -> None:
        """ """
        super(CheetahGui, self).__init__()
        self._ui: Any = uic.loadUi(
            (pathlib.Path(cheetah_src_path) / "../ui_src/cheetahgui.ui").resolve(), self
        )
        self.show()

        self._select_experiment()
        self.setWindowTitle(f"Cheetah GUI: {self.experiment.get_working_directory()}")
        self._crawler_csv_filename: pathlib.Path = (
            self.experiment.get_crawler_csv_filename()
        )

        self._table: Any = self._ui.table_status
        self._table_data: List[Dict[str, Any]] = []
        self._table.horizontalHeader().setDefaultSectionSize(
            self.width() // self._table.columnCount()
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

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh_table)

        self._refresh_table()

        # Connect front panel buttons to actions
        self._ui.button_refresh.clicked.connect(self._refresh_table)
        self._ui.button_run_cheetah.clicked.connect(self._process_runs)
        self._ui.button_index.clicked.connect(self._pass)
        self._ui.button_view_hits.clicked.connect(self._pass)
        self._ui.button_sum_blanks.clicked.connect(self._pass)
        self._ui.button_sum_hits.clicked.connect(self._pass)
        self._ui.button_peak_powder.clicked.connect(self._pass)
        self._ui.button_peakogram.clicked.connect(self._pass)

        # File menu actions
        self._ui.menu_file_start_crawler.triggered.connect(self._start_crawler)
        self._ui.menu_file_new_geometry.triggered.connect(self._pass)
        self._ui.menu_file_modify_beamline_configuration.triggered.connect(self._pass)

        # Cheetah menu actions
        self._ui.menu_cheetah_process_selected.triggered.connect(self._process_runs)
        self._ui.menu_cheetah_relabel.triggered.connect(self._pass)
        self._ui.menu_cheetah_autorun.triggered.connect(self._pass)
        self._ui.menu_modify_config_files.triggered.connect(self._pass)

        # CrystFEL actions
        self._ui.menu_crystfel_index.triggered.connect(self._pass)
        self._ui.menu_crystfel_view_indexing_results.triggered.connect(self._pass)
        self._ui.menu_crystfel_view_indexing_pick.triggered.connect(self._pass)
        self._ui.menu_crystfel_cell_explorer.triggered.connect(self._pass)
        self._ui.menu_crystfel_cell_explorer_pick.triggered.connect(self._pass)
        self._ui.menu_crystfel_merge_streams.triggered.connect(self._pass)
        self._ui.menu_crystfel_list_events.triggered.connect(self._pass)
        self._ui.menu_crystfel_list_files.triggered.connect(self._pass)

        # Mask menu actions
        self._ui.menu_mask_maker.triggered.connect(self._pass)
        self._ui.menu_mask_combine.triggered.connect(self._pass)
        self._ui.menu_mask_view.triggered.connect(self._pass)

        # Analysis menu items
        self._ui.menu_analysis_hitrate.triggered.connect(self._pass)
        self._ui.menu_analysis_peakogram.triggered.connect(self._pass)
        self._ui.menu_analysis_resolution.triggered.connect(self._pass)
        self._ui.menu_analysis_saturation.triggered.connect(self._pass)

        # Powder menu actions
        self._ui.menu_powder_hits_sum.triggered.connect(self._pass)
        self._ui.menu_powder_blanks_sum.triggered.connect(self._pass)
        self._ui.menu_powder_peaks_hits.triggered.connect(self._pass)
        self._ui.menu_powder_peaks_blanks.triggered.connect(self._pass)

        # Log menu actions
        self._ui.menu_log_batch.triggered.connect(self._pass)
        self._ui.menu_log_cheetah.triggered.connect(self._pass)
        self._ui.menu_log_cheetah_status.triggered.connect(self._pass)
        self._ui.menu_log_cheetah.setEnabled(False)
        self._ui.menu_log_cheetah_status.setEnabled(False)

        # Disable action commands until enabled
        self._ui.button_run_cheetah.setEnabled(False)
        self._ui.button_index.setEnabled(False)
        self._ui.menu_file_start_crawler.setEnabled(False)
        self._ui.menu_cheetah_process_selected.setEnabled(False)
        self._ui.menu_cheetah_autorun.setEnabled(False)
        self._ui.menu_cheetah_relabel.setEnabled(False)
        self._ui.menu_file_command.triggered.connect(self._enable_commands)

    def _pass(self) -> None:
        pass

    def _crawler_gui_closed(self) -> None:
        print("Crawler closed")

    def _enable_commands(self) -> None:
        self._ui.button_run_cheetah.setEnabled(True)
        # self._ui.button_index.setEnabled(True)
        self._ui.menu_file_start_crawler.setEnabled(True)
        self._ui.menu_cheetah_process_selected.setEnabled(True)
        # self._ui.menu_cheetah_autorun.setEnabled(True)
        # self._ui.menu_cheetah_relabel.setEnabled(True)

    def _exit(self) -> None:
        print("Bye bye.")
        sys.exit(0)

    def _process_runs(self) -> None:
        selected_rows: List[int] = sorted(
            (index.row() for index in self._table.selectionModel().selectedRows())
        )
        row: int
        for row in selected_rows:
            table_id: str = self._table.item(row, 0).text()
            run_id: str = self.experiment.crawler_table_id_to_raw_id(table_id)
            self.experiment.process_run(
                run_id, self.experiment._last_process_config_filename, "test"
            )
        # TODO: move process to a separate thread, add process dialog

    def _refresh_table(self) -> None:
        if not self._crawler_csv_filename.exists():
            self._refresh_timer.start(60000)
            return
        self._table.setSortingEnabled(False)
        n_columns: int = self._table.columnCount()
        column_names: List[str] = [
            self._table.horizontalHeaderItem(i).text() for i in range(n_columns)
        ]
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
        for column, name in enumerate(column_names):
            if name in self._table_data[0].keys():
                for row, data in enumerate(self._table_data):
                    value: str = data[name]
                    item: Any = QtWidgets.QTableWidgetItem()
                    try:
                        item.setData(QtCore.Qt.DisplayRole, float(value))
                    except ValueError:
                        item.setText(value)
                    self._table.setItem(row, column, item)

        self._table.resizeRowsToContents()
        self._table.setSortingEnabled(True)
        print(f"Table refreshed at {datetime.now()}")

        self._refresh_timer.start(60000)

    def _select_experiment(self) -> None:
        if pathlib.Path("./crawler.config").is_file():
            working_directory: pathlib.Path = pathlib.Path.cwd()
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
        print("Starting crawler")
        self.crawler_window = CrawlerGui(self.experiment, self)
        self.crawler_window.show()


@click.command()  # type: ignore
def main() -> None:
    """ """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = CheetahGui()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

import click  # type: ignore
import sys
import csv
import time
import pathlib
from datetime import datetime
import shutil

from typing import Any, List, Dict, Text, TextIO
from PyQt5 import QtCore, QtGui, QtWidgets, uic  # type: ignore
from cheetah.dialogs import setup_dialogs
from cheetah import __file__ as cheetah_src_path
from cheetah.crawlers import facilities


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

        self._setup_experiment()

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

        self._refresh_timer: Any = QtCore.QTimer()
        self._refresh_timer.timeout.connect(self._refresh_table)

        self._refresh_table()

        # Connect front panel buttons to actions
        self._ui.button_refresh.clicked.connect(self._refresh_table)
        self._ui.button_run_cheetah.clicked.connect(self._pass)
        self._ui.button_index.clicked.connect(self._pass)
        self._ui.button_view_hits.clicked.connect(self._pass)
        self._ui.button_sum_blanks.clicked.connect(self._pass)
        self._ui.button_sum_hits.clicked.connect(self._pass)
        self._ui.button_peak_powder.clicked.connect(self._pass)
        self._ui.button_peakogram.clicked.connect(self._pass)

        # File menu actions
        self._ui.menu_file_start_crawler.triggered.connect(self._pass)
        self._ui.menu_file_new_geometry.triggered.connect(self._pass)
        self._ui.menu_file_modify_beamline_configuration.triggered.connect(self._pass)

        # Cheetah menu actions
        self._ui.menu_cheetah_process_selected.triggered.connect(self._pass)
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

    def _exit(self) -> None:
        print("Bye bye.")
        sys.exit(0)

    def _select_experiment(self) -> pathlib.Path:
        dialog: setup_dialogs.ExperimentSelectionDialog = (
            setup_dialogs.ExperimentSelectionDialog(self)
        )
        if dialog.exec() == 0:
            print("Catch you another time.")
            self._exit()
        return dialog.get_experiment()

    def _parse_config(self) -> Dict[str, str]:
        config: Dict[str, str] = {}
        fh: TextIO
        with open(self._crawler_config_filename, "r") as fh:
            line: str
            for line in fh:
                line_items: List[str] = line.split("=")
                if len(line_items) == 2:
                    config[line_items[0].strip()] = line_items[1].strip()
        return config

    def _resolve_path(
        self, path: pathlib.Path, parent_path: pathlib.Path
    ) -> pathlib.Path:
        if path.is_absolute():
            return path
        else:
            return (self._working_directory / path).resolve()

    def _setup_experiment(self) -> None:
        if not pathlib.Path("./crawler.config").is_file():
            self._working_directory = self._select_experiment()
        else:
            self._working_directory = pathlib.Path.cwd()
        self._crawler_config_filename: pathlib.Path = (
            self._working_directory / "crawler.config"
        )
        self._process_directory: pathlib.Path = (
            self._working_directory / "../process"
        ).resolve()
        if self._crawler_config_filename.exists():
            self._load_existing_experiment()
        else:
            self._setup_new_experiment()
        self.setWindowTitle("Cheetah GUI: {self._working_directory}")
        self._crawler_csv_filename: pathlib.Path = (
            self._working_directory / "crawler.txt"
        )

    def _load_existing_experiment(self) -> None:
        print(
            f"Going to selected experiment: {self._working_directory}\n"
            f"Loading configuration file: {self._crawler_config_filename}"
        )
        self._config: Dict[str, str] = self._parse_config()
        self._last_cheetah_config_filename: pathlib.Path = self._resolve_path(
            pathlib.Path(self._config["cheetahini"]), self._process_directory
        )
        self._last_geometry: pathlib.Path = self._resolve_path(
            pathlib.Path(self._config["geometry"]), self._working_directory
        )
        self._last_tag: str = self._config["cheetahtag"]

    def _setup_new_experiment(self) -> None:
        dialog: setup_dialogs.SetupNewExperimentDialog = (
            setup_dialogs.SetupNewExperimentDialog(self._working_directory, self)
        )
        if dialog.exec() == 0:
            self._setup_experiment()
            return
        else:
            new_experiment_config: Dict[str, str] = dialog.get_config()
        print("Setting up new experiment\n")
        print(
            f"Creating new Cheetah directory:\n{new_experiment_config['output_dir']}\n"
        )
        self._working_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "gui"
        )
        self._working_directory.mkdir(parents=True, exist_ok=False)

        self._hdf5_directory: pathlib.Path = (
            pathlib.Path(new_experiment_config["output_dir"]) / "hdf5"
        )
        self._hdf5_directory.mkdir(parents=True, exist_ok=False)

        self._calib_directory: pathlib.Path = (
            pathlib.Path(new_experiment_config["output_dir"]) / "calib"
        )
        self._calib_directory.mkdir(parents=True, exist_ok=False)

        self._process_directory = (
            pathlib.Path(new_experiment_config["output_dir"]) / "process"
        )
        self._process_directory.mkdir(parents=True, exist_ok=False)

        print(
            f"Copying {new_experiment_config['detector']} geometry and mask to \n"
            f"{self._calib_directory}\n"
        )
        resource: str
        for resource in facilities[new_experiment_config["facility"]]["instruments"][
            new_experiment_config["instrument"]
        ]["detectors"][new_experiment_config["detector"]]["resources"]:
            shutil.copyfile(
                pathlib.Path(new_experiment_config["cheetah_resources"]) / resource,
                self._calib_directory / resource,
            )

    def _enable_commands(self) -> None:
        self._ui.button_run_cheetah.setEnabled(True)
        self._ui.button_index.setEnabled(True)
        self._ui.menu_file_start_crawler.setEnabled(True)
        self._ui.menu_cheetah_process_selected.setEnabled(True)
        self._ui.menu_cheetah_autorun.setEnabled(True)
        self._ui.menu_cheetah_relabel.setEnabled(True)

    def _refresh_table(self) -> None:
        if not self._crawler_csv_filename.exists():
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


@click.command()  # type: ignore
def main() -> None:
    """ """
    app: Any = QtWidgets.QApplication(sys.argv)
    _ = CheetahGui()
    sys.exit(app.exec_())


if __name__ == "__main__":
    main()

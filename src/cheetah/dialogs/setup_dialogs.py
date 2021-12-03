import os
import pathlib

from PyQt5 import QtWidgets  # type: ignore
from typing import Any, List, TextIO, Union, Callable

from cheetah.dialogs.generic_dialogs import PathDoesNotExistDialog
from cheetah.crawlers import facilities
from cheetah import __file__ as cheetah_src_path
from cheetah.experiment import TypeExperimentConfig


class ExperimentSelectionDialog(QtWidgets.QDialog):  # type: ignore
    """
    See documentation of the `__init__` function
    """

    def __init__(self, parent: Any = None) -> None:
        """ """
        super(ExperimentSelectionDialog, self).__init__(parent)
        self.setWindowTitle("Cheetah GUI experiment selector")

        self._previous_experiments_label: Any = QtWidgets.QLabel("Previous experiments")
        self._previous_experiments_cb: Any = QtWidgets.QComboBox()
        previous_experiments_list: List[str] = self._get_previous_experiments_list()
        self._previous_experiments_cb.addItems(previous_experiments_list)
        layout: Any = QtWidgets.QVBoxLayout(self)
        layout.addWidget(self._previous_experiments_label)
        layout.addWidget(self._previous_experiments_cb)

        self._go_to_selected_experiment_button: Any = QtWidgets.QPushButton(
            "Go to selected experiment", self
        )
        if len(previous_experiments_list) == 0:
            self._go_to_selected_experiment_button.setEnabled(False)
        self._go_to_selected_experiment_button.clicked.connect(
            self._go_to_selected_experiment
        )
        self._setup_new_experiment_button: Any = QtWidgets.QPushButton(
            "Set up new experiment", self
        )
        self._setup_new_experiment_button.clicked.connect(self._setup_new_experiment)
        self._find_different_experiment_button: Any = QtWidgets.QPushButton(
            "Find a different experiment", self
        )
        self._find_different_experiment_button.clicked.connect(
            self._find_different_experiment
        )
        self._cancel_button: Any = QtWidgets.QPushButton("Cancel", self)
        self._cancel_button.clicked.connect(self._cancel)
        buttons_layout: Any = QtWidgets.QHBoxLayout()
        buttons_layout.addWidget(self._go_to_selected_experiment_button)
        buttons_layout.addWidget(self._setup_new_experiment_button)
        buttons_layout.addWidget(self._find_different_experiment_button)
        buttons_layout.addWidget(self._cancel_button)

        layout.addLayout(buttons_layout)

    def _go_to_selected_experiment(self) -> None:
        selected_path: pathlib.Path = pathlib.Path(
            self._previous_experiments_cb.currentText()
        )
        if selected_path.exists():
            if (selected_path / "crawler.config").is_file():
                self._selected_experiment: pathlib.Path = selected_path
                self.get_experiment()
                self.done(1)
            else:
                PathDoesNotExistDialog(selected_path, "crawler.config", self).exec()
        else:
            PathDoesNotExistDialog(selected_path, None, self).exec()

    def _find_different_experiment(self) -> None:
        file_selection_dialog: Any = QtWidgets.QFileDialog(self, "Open file", ".")
        file_selection_dialog.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        file_selection_dialog.setNameFilter("crawler.config")
        if file_selection_dialog.exec_():
            self._selected_experiment = pathlib.Path(
                file_selection_dialog.selectedFiles()[0]
            ).parent
            self.done(1)

    def _setup_new_experiment(self) -> None:
        # Hack to get current directory without resolving links at psana
        # instead of using pathlib.Path.cwd()
        cwd: str = os.environ["PWD"]
        directory_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select new experiment directory", cwd
        )
        directory_selection_dialog.setFileMode(QtWidgets.QFileDialog.Directory)
        if directory_selection_dialog.exec_():
            self._selected_experiment = pathlib.Path(
                directory_selection_dialog.selectedFiles()[0]
            )
            self.done(1)

    def _cancel(self) -> None:
        self.done(0)

    def _get_previous_experiments_list(self) -> List[str]:
        logfile_path: pathlib.Path = pathlib.Path.expanduser(
            pathlib.Path("~/.cheetah-crawler2")
        )
        if pathlib.Path(logfile_path).is_file():
            fh: TextIO
            with open(logfile_path) as fh:
                return [line.strip() for line in fh.readlines()]
        else:
            return []

    def get_experiment(self) -> pathlib.Path:
        return self._selected_experiment


class SetupNewExperimentDialog(QtWidgets.QDialog):  # type: ignore
    """
    See documentation of the `__init__` function
    """

    def __init__(self, path: pathlib.Path, parent: Any = None) -> None:
        """ """
        super(SetupNewExperimentDialog, self).__init__(parent)
        self.setWindowTitle("Set up new experiment")
        self.resize(800, 300)
        self._path: pathlib.Path = path
        layout: Any = QtWidgets.QVBoxLayout(self)

        self._form: Any = QtWidgets.QGroupBox()
        form_layout: Any = QtWidgets.QFormLayout()

        self._facility_cb: Any = QtWidgets.QComboBox()
        self._facility_cb.addItem("")
        self._facility_cb.addItems(facilities.keys())
        self._facility_cb.currentIndexChanged.connect(self._facility_changed)
        form_layout.addRow("Facility: ", self._facility_cb)

        self._instrument_cb: Any = QtWidgets.QComboBox()
        self._instrument_cb.setEnabled(False)
        self._instrument_cb.currentIndexChanged.connect(self._instrument_changed)
        form_layout.addRow("Instrument: ", self._instrument_cb)

        self._detector_cb: Any = QtWidgets.QComboBox()
        self._detector_cb.setEnabled(False)
        form_layout.addRow("Detector: ", self._detector_cb)

        self._raw_directory_le: Any = QtWidgets.QLineEdit()
        self._raw_directory_le.setReadOnly(True)
        self._raw_directory_le.textChanged.connect(self._raw_directory_changed)
        self._raw_directory_button: Any = QtWidgets.QPushButton("Browse")
        self._raw_directory_button.clicked.connect(self._select_raw_directory)
        raw_directory_layout: Any = QtWidgets.QHBoxLayout()
        raw_directory_layout.addWidget(self._raw_directory_le)
        raw_directory_layout.addWidget(self._raw_directory_button)
        form_layout.addRow("Raw data directory: ", raw_directory_layout)

        self._experiment_id_le: Any = QtWidgets.QLineEdit()
        self._experiment_id_le.textChanged.connect(self._check_config)
        form_layout.addRow("Experiment ID: ", self._experiment_id_le)

        self._cheetah_directory_le: Any = QtWidgets.QLineEdit()
        self._cheetah_directory_le.setReadOnly(True)
        self._cheetah_directory_le.setText(str(path / "cheetah"))
        form_layout.addRow("Output directory: ", self._cheetah_directory_le)

        self._cheetah_resources_le: Any = QtWidgets.QLineEdit()
        self._cheetah_resources_le.setReadOnly(True)
        self._cheetah_resources_button: Any = QtWidgets.QPushButton("Browse")
        self._cheetah_resources_button.clicked.connect(self._select_cheetah_resources)
        cheetah_resources_layout: Any = QtWidgets.QHBoxLayout()
        cheetah_resources_layout.addWidget(self._cheetah_resources_le)
        cheetah_resources_layout.addWidget(self._cheetah_resources_button)
        cheetah_resources_directory: Union[
            pathlib.Path, None
        ] = self._guess_cheetah_resources_directory()
        if cheetah_resources_directory:
            self._cheetah_resources_le.setText(str(cheetah_resources_directory))
            self._cheetah_resources_button.setEnabled(False)
        form_layout.addRow("Cheetah resources directory: ", cheetah_resources_layout)

        self._button_box: Any = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Cancel | QtWidgets.QDialogButtonBox.Ok
        )
        self._button_box.accepted.connect(self._accept)
        self._button_box.rejected.connect(self._cancel)

        form_layout.addRow("Proceed? ", self._button_box)

        self._form.setLayout(form_layout)
        layout.addWidget(self._form)

        self._check_config()

    def _accept(self) -> None:
        self.done(1)

    def _cancel(self) -> None:
        self.done(0)

    def _check_config(self) -> None:
        self._config: TypeExperimentConfig = {
            "facility": self._facility_cb.currentText(),
            "instrument": self._instrument_cb.currentText(),
            "detector": self._detector_cb.currentText(),
            "raw_dir": self._raw_directory_le.text(),
            "experiment_id": self._experiment_id_le.text(),
            "output_dir": self._cheetah_directory_le.text(),
            "cheetah_resources": self._cheetah_resources_le.text(),
        }
        if "" in self._config.values():
            self._button_box.buttons()[0].setEnabled(False)
        else:
            self._button_box.buttons()[0].setEnabled(True)

    def _facility_changed(self) -> None:
        self._facility: str = self._facility_cb.currentText()
        self._instrument_cb.clear()
        if self._facility:
            self._instrument_cb.setEnabled(True)
            self._instrument_cb.addItems(
                facilities[self._facility]["instruments"].keys()
            )
            instrument: Union[str, None] = self._guess_instrument()
            if instrument:
                index: int = self._instrument_cb.findText(instrument)
                if index >= 0:
                    self._instrument_cb.setCurrentIndex(index)
        else:
            self._instrument_cb.setEnabled(False)
        possible_raw_directory: Union[
            pathlib.Path, None
        ] = self._guess_raw_data_directory()
        if possible_raw_directory is not None and possible_raw_directory.is_dir():
            self._raw_directory_le.setText(str(possible_raw_directory))

        self._check_config()

    def _guess_cheetah_resources_directory(self) -> Union[pathlib.Path, None]:
        path: pathlib.Path = pathlib.Path(cheetah_src_path).parent / "resources"
        if path.is_dir():
            return path
        else:
            return None

    def _guess_experiment_id(self, path: pathlib.Path) -> Union[str, None]:
        if self._facility:
            function: Callable[[pathlib.Path], str] = facilities[self._facility][
                "guess_experiment_id"
            ]

            return function(path)
        else:
            return None

    def _guess_instrument(self) -> Union[str, None]:
        instrument: str
        for instrument in facilities[self._facility]["instruments"].keys():
            if "/" + instrument in str(self._path) or "/" + instrument.lower() in str(
                self._path
            ):
                return instrument
        return None

    def _guess_raw_data_directory(self) -> Union[pathlib.Path, None]:
        if self._facility:
            function: Callable[[pathlib.Path], pathlib.Path] = facilities[
                self._facility
            ]["guess_raw_directory"]

            return function(self._path)
        else:
            return None

    def _instrument_changed(self) -> None:
        self._instrument: str = self._instrument_cb.currentText()
        self._detector_cb.clear()
        if self._instrument:
            self._detector_cb.setEnabled(True)
            self._detector_cb.addItems(
                facilities[self._facility]["instruments"][self._instrument]["detectors"]
            )
        else:
            self._detector_cb.setEnabled(False)
        self._check_config()

    def _raw_directory_changed(self) -> None:
        raw_directory: str = self._raw_directory_le.text()
        possible_experiment_id: Union[str, None] = self._guess_experiment_id(
            pathlib.Path(raw_directory)
        )
        if possible_experiment_id:
            self._experiment_id_le.setText(possible_experiment_id)

    def _select_raw_directory(self) -> None:
        path: str = self._raw_directory_le.text()
        if not path:
            path = str(self._path)

        directory_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select directory with raw data", path
        )
        directory_selection_dialog.setFileMode(QtWidgets.QFileDialog.Directory)
        if directory_selection_dialog.exec_():
            path = directory_selection_dialog.selectedFiles()[0]
            self._raw_directory_le.setText(path)
        self._check_config()

    def _select_cheetah_resources(self) -> None:
        directory_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select directory with Cheetah resources", "~"
        )
        directory_selection_dialog.setFileMode(QtWidgets.QFileDialog.Directory)
        if directory_selection_dialog.exec_():
            path: str = directory_selection_dialog.selectedFiles()[0]
            self._cheetah_resources_le.setText(path)
        self._check_config()

    def get_config(self) -> TypeExperimentConfig:
        self._check_config()
        return self._config

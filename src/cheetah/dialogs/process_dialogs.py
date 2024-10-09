"""
Process dialogs.

This modules contains dialogs which allow setting up processing configuration
parameters and launching data processing.
"""

import logging
import pathlib
from typing import Any, Dict, Optional, TextIO, Union

import ruamel.yaml
from PyQt5 import QtCore, QtGui, QtWidgets  # type: ignore

from cheetah.dialogs.generic_dialogs import PathDoesNotExistDialog
from cheetah.process import IndexingConfig, ProcessingConfig

logger: logging.Logger = logging.getLogger(__name__)


class RunProcessingDialog(QtWidgets.QDialog):  # type: ignore
    """
    See documentation of the `__init__` function
    """

    def __init__(
        self,
        last_config: ProcessingConfig,
        streaming: bool = False,
        process_hits_option: bool = False,
        parent: Any = None,
    ) -> None:
        """
        Run processing dialog.

        This dialog is shown when data processing is triggered from Cheetah GUI. It
        allows to select Cheetah processing config template file, dataset tag, geometry
        file and mask file.

        Arguments:

            last_config: A [TypeProcessingConfig][cheetah.process.TypeProcessingConfig]
                dictionary containing the latest used processing configuration
                parameteres, which will be used to pre-fill the dialog form.

            parent: Parent QWidget. Defaults to None.
        """
        super(RunProcessingDialog, self).__init__(parent)
        self.setWindowTitle("Run Cheetah")
        self.resize(800, 250)
        layout: Any = QtWidgets.QVBoxLayout(self)

        self._form: Any = QtWidgets.QGroupBox()
        form_layout: Any = QtWidgets.QFormLayout()

        self._process_hits_cb: Any = QtWidgets.QCheckBox("")
        self._process_hits_cb.setChecked(False)
        if process_hits_option:
            form_layout.addRow("Process hits only: ", self._process_hits_cb)

        self._template_le: Any = QtWidgets.QLineEdit()
        self._template_le.setText(last_config.config_template)
        self._template_le.textChanged.connect(self._check_config)
        self._template_button: Any = QtWidgets.QPushButton("Browse")
        self._template_button.clicked.connect(self._select_config_template)
        template_layout: Any = QtWidgets.QHBoxLayout()
        template_layout.addWidget(self._template_le)
        template_layout.addWidget(self._template_button)
        form_layout.addRow("Cheetah config file*: ", template_layout)

        self._edit_pf_parameters_button: Any = QtWidgets.QPushButton(
            "Edit peakfinder parameters"
        )
        self._edit_pf_parameters_button.clicked.connect(self._edit_pf_parameters)
        form_layout.addRow("", self._edit_pf_parameters_button)

        self._tag_le: Any = QtWidgets.QLineEdit()
        self._tag_le.setText(last_config.tag)
        form_layout.addRow("Dataset name: ", self._tag_le)

        self._geometry_le: Any = QtWidgets.QLineEdit()
        self._geometry_le.setText(last_config.geometry)
        self._geometry_le.textChanged.connect(self._check_config)
        self._geometry_button: Any = QtWidgets.QPushButton("Browse")
        self._geometry_button.clicked.connect(self._select_geometry_file)
        geometry_layout: Any = QtWidgets.QHBoxLayout()
        geometry_layout.addWidget(self._geometry_le)
        geometry_layout.addWidget(self._geometry_button)
        form_layout.addRow("Geometry file*: ", geometry_layout)

        self._mask_le: Any = QtWidgets.QLineEdit()
        self._mask_le.setText(last_config.mask)
        self._mask_button: Any = QtWidgets.QPushButton("Browse")
        self._mask_button.clicked.connect(self._select_mask_file)
        mask_layout: Any = QtWidgets.QHBoxLayout()
        mask_layout.addWidget(self._mask_le)
        mask_layout.addWidget(self._mask_button)
        form_layout.addRow("Mask file: ", mask_layout)

        self._cell_file_le: Any = QtWidgets.QLineEdit()
        self._cell_file_button: Any = QtWidgets.QPushButton("Browse")
        self._cell_file_button.clicked.connect(self._select_cell_file)
        cell_file_layout: Any = QtWidgets.QHBoxLayout()
        cell_file_layout.addWidget(self._cell_file_le)
        cell_file_layout.addWidget(self._cell_file_button)
        self._indexing_le: Any = QtWidgets.QLineEdit()
        self._extra_args_le: Any = QtWidgets.QLineEdit()

        self._streaming: bool = streaming
        if self._streaming:
            form_layout.addRow("Unit cell file: ", cell_file_layout)
            form_layout.addRow("Indexing methods (--indexing=): ", self._indexing_le)
            form_layout.addRow("Extra indexamajig arguments: ", self._extra_args_le)
            if last_config.indexing_config:
                self._cell_file_le.setText(last_config.indexing_config.cell_file)
                self._indexing_le.setText(last_config.indexing_config.indexing)
                self._extra_args_le.setText(last_config.indexing_config.extra_args)

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
        # Checks the dialog form, if all fields are filled correctly, i.e. all
        # specified files exist, exits with signal 1. Otherwise shows path not found
        # message.
        self._check_config()
        template_file: pathlib.Path = pathlib.Path(self._config.config_template)
        if not template_file.is_file():
            PathDoesNotExistDialog(
                template_file.parent, template_file.name, self
            ).exec()
            return
        geometry_file: pathlib.Path = pathlib.Path(self._config.geometry)
        if not geometry_file.is_file():
            PathDoesNotExistDialog(
                geometry_file.parent, geometry_file.name, self
            ).exec()
            return
        if self._config.mask:
            mask_file: pathlib.Path = pathlib.Path(self._config.mask)
            if not mask_file.is_file():
                PathDoesNotExistDialog(mask_file.parent, mask_file.name, self).exec()
                return
        if self._streaming and self._config.indexing_config:
            if self._config.indexing_config.cell_file:
                cell_file: pathlib.Path = pathlib.Path(
                    self._config.indexing_config.cell_file
                )
                if not cell_file.is_file():
                    PathDoesNotExistDialog(
                        cell_file.parent, cell_file.name, self
                    ).exec()
                    return
        self.done(1)

    def _cancel(self) -> None:
        # Exits with signal 0.
        self.done(0)

    def _check_config(self) -> None:
        # Checks that all required fields are filled, if not disables "OK" button.
        if self._streaming:
            indexing_config: Optional[IndexingConfig] = IndexingConfig(
                cell_file=self._cell_file_le.text(),
                indexing=self._indexing_le.text().replace(" ", ""),
                extra_args=self._extra_args_le.text(),
            )
        else:
            indexing_config = None
        self._config: ProcessingConfig = ProcessingConfig(
            config_template=self._template_le.text(),
            tag=self._tag_le.text(),
            geometry=self._geometry_le.text(),
            mask=self._mask_le.text(),
            indexing_config=indexing_config,
            event_list=None,
            write_data_files=True,
        )
        if not self._config.config_template or not self._config.geometry:
            self._button_box.buttons()[0].setEnabled(False)
        else:
            self._button_box.buttons()[0].setEnabled(True)

    def _edit_pf_parameters(self) -> None:
        # Opens a dialog to edit peakfinder parameters.
        config_file: pathlib.Path = pathlib.Path(self._template_le.text())
        dialog: PeakfinderParametersDialog = PeakfinderParametersDialog(
            config_file, self
        )
        if dialog.exec():
            self._template_le.setText(str(dialog.get_new_config_file()))
            self._check_config()

    def _select_config_template(self) -> None:
        # Opens file selection dialog to select *.yaml file.
        path: pathlib.Path = pathlib.Path(self._template_le.text()).parent
        if not path.exists():
            path = pathlib.Path.cwd()
        file_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select cheetah config file", str(path)
        )
        file_selection_dialog.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        file_selection_dialog.setNameFilter("*.yaml")
        if file_selection_dialog.exec_():
            self._template_le.setText(file_selection_dialog.selectedFiles()[0])
            self._check_config()

    def _select_geometry_file(self) -> None:
        # Opens file selection dialog to select *.geom file.
        path: pathlib.Path = pathlib.Path(self._geometry_le.text()).parent
        if not path.exists():
            path = pathlib.Path.cwd()
        file_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select geometry file", str(path)
        )
        file_selection_dialog.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        file_selection_dialog.setNameFilter("*.geom")
        if file_selection_dialog.exec_():
            self._geometry_le.setText(file_selection_dialog.selectedFiles()[0])
            self._check_config()

    def _select_mask_file(self) -> None:
        # Opens file selection dialog to select *.h5 file.
        path: pathlib.Path = pathlib.Path(self._mask_le.text()).parent
        if not path.exists():
            path = pathlib.Path.cwd()
        file_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select mask file", str(path)
        )
        file_selection_dialog.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        file_selection_dialog.setNameFilter("*.h5")
        if file_selection_dialog.exec_():
            self._mask_le.setText(file_selection_dialog.selectedFiles()[0])
            self._check_config()

    def _select_cell_file(self) -> None:
        # Opens file selection dialog to select *.cell or *.pdb file.
        path: pathlib.Path = pathlib.Path(self._cell_file_le.text()).parent
        if not path.exists():
            path = pathlib.Path.cwd()
        file_selection_dialog: Any = QtWidgets.QFileDialog(
            self, "Select unit cell file", str(path)
        )
        file_selection_dialog.setFileMode(QtWidgets.QFileDialog.ExistingFile)
        file_selection_dialog.setNameFilter("*.cell *.pdb")
        if file_selection_dialog.exec_():
            self._cell_file_le.setText(file_selection_dialog.selectedFiles()[0])
            self._check_config()

    def get_config(
        self,
    ) -> ProcessingConfig:
        """
        Get processing config.

        This function is called when the dialog exits with signal 1, which means that
        all selected processing configuration parameters are valid.

        Returns:
            A [TypeProcessingConfig][cheetah.process.TypeProcessingConfig]
            dictionary containing selected processing configuration parameters.
        """
        return self._config

    def process_hits(self) -> bool:
        """
        Get process hits option.

        This function is called when the dialog exits with signal 1, which means that
        all selected processing configuration parameters are valid.

        Returns:
            A boolean value indicating if the process hits option is selected.
        """
        return self._process_hits_cb.isChecked()


class PeakfinderParametersDialog(QtWidgets.QDialog):
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self, config_file: Union[pathlib.Path, str], parent: Any = None
    ) -> None:
        """
        Peakfinder parameters dialog.

        This dialog is shown when the user clicks on the "Edit peakfinder parameters"
        button in the "Run processing" dialog. It allows to change peakfinder8 peak
        detection parameters in the provided config file and save the changes.

        Arguments:

            config_file: Path to the config file.

            parent: Parent QWidget. Defaults to None.
        """
        super(PeakfinderParametersDialog, self).__init__(parent)
        self.setWindowTitle("Set peakfinder8 parameters")
        self.resize(800, 400)

        self._config_file: pathlib.Path = pathlib.Path(config_file)
        self._yaml: ruamel.yaml.YAML = ruamel.yaml.YAML(typ="jinja2")
        self._yaml.indent(mapping=2, sequence=1, offset=2)
        self._yaml.preserve_quotes = True
        if not self._config_file.is_file():
            PathDoesNotExistDialog(
                self._config_file.parent, self._config_file.name, self
            ).exec()
            self.done(0)

        fh: TextIO
        with open(self._config_file, "r") as fh:
            self._config: Dict[str, Any] = self._yaml.load(fh)
        self._new_config_file: pathlib.Path = self._config_file.parent / (
            self._config_file.stem + "-new.yaml"
        )

        pf8_config: Dict[str, Any] = self._config["peakfinder8_peak_detection"]

        layout: Any = QtWidgets.QVBoxLayout(self)
        self._form: Any = QtWidgets.QGroupBox()
        form_layout: Any = QtWidgets.QFormLayout()

        self._float_regex: Any = QtCore.QRegExp(r"[0-9.,]+")
        self._float_validator: Any = QtGui.QRegExpValidator()
        self._float_validator.setRegExp(self._float_regex)

        self._int_regex: Any = QtCore.QRegExp(r"[0-9]+")
        self._int_validator: Any = QtGui.QRegExpValidator()
        self._int_validator.setRegExp(self._int_regex)

        self._adc_threshold_le: Any = QtWidgets.QLineEdit()
        self._adc_threshold_le.setValidator(self._float_validator)
        self._adc_threshold_le.setText(str(pf8_config["adc_threshold"]))
        self._adc_threshold_le.textChanged.connect(self._check_config)
        form_layout.addRow("ADC threshold: ", self._adc_threshold_le)

        self._min_snr_le: Any = QtWidgets.QLineEdit()
        self._min_snr_le.setValidator(self._float_validator)
        self._min_snr_le.setText(str(pf8_config["minimum_snr"]))
        self._min_snr_le.textChanged.connect(self._check_config)
        form_layout.addRow("Minimum SNR: ", self._min_snr_le)

        self._min_pix_count_le: Any = QtWidgets.QLineEdit()
        self._min_pix_count_le.setValidator(self._int_validator)
        self._min_pix_count_le.setText(str(pf8_config["min_pixel_count"]))
        self._min_pix_count_le.textChanged.connect(self._check_config)
        form_layout.addRow("Minimum pixel count: ", self._min_pix_count_le)

        self._max_pix_count_le: Any = QtWidgets.QLineEdit()
        self._max_pix_count_le.setValidator(self._int_validator)
        self._max_pix_count_le.setText(str(pf8_config["max_pixel_count"]))
        self._max_pix_count_le.textChanged.connect(self._check_config)
        form_layout.addRow("Maximum pixel count: ", self._max_pix_count_le)

        self._local_bg_radius_le: Any = QtWidgets.QLineEdit()
        self._local_bg_radius_le.setValidator(self._int_validator)
        self._local_bg_radius_le.setText(str(pf8_config["local_bg_radius"]))
        self._local_bg_radius_le.textChanged.connect(self._check_config)
        form_layout.addRow("Local background radius: ", self._local_bg_radius_le)

        self._min_res_le: Any = QtWidgets.QLineEdit()
        self._min_res_le.setValidator(self._int_validator)
        self._min_res_le.setText(str(pf8_config["min_res"]))
        self._min_res_le.textChanged.connect(self._check_config)
        form_layout.addRow("Minimum resolution (pixels): ", self._min_res_le)

        self._max_res_le: Any = QtWidgets.QLineEdit()
        self._max_res_le.setValidator(self._int_validator)
        self._max_res_le.setText(str(pf8_config["max_res"]))
        self._max_res_le.textChanged.connect(self._check_config)
        form_layout.addRow("Maximum resolution (pixels): ", self._max_res_le)

        self._min_peaks_for_hit: Any = QtWidgets.QLineEdit()
        self._min_peaks_for_hit.setValidator(self._int_validator)
        self._min_peaks_for_hit.setText(
            str(self._config["crystallography"]["min_num_peaks_for_hit"])
        )
        self._min_peaks_for_hit.textChanged.connect(self._check_config)
        form_layout.addRow("Minimum number of peaks for hit: ", self._min_peaks_for_hit)

        self._button_box: Any = QtWidgets.QDialogButtonBox(
            QtWidgets.QDialogButtonBox.Cancel | QtWidgets.QDialogButtonBox.Ok
        )
        self._button_box.accepted.connect(self._accept)
        self._button_box.rejected.connect(self._cancel)

        form_layout.addRow("Save new config file?", self._button_box)

        self._form.setLayout(form_layout)
        layout.addWidget(self._form)

    def _accept(self) -> None:
        # Checks the dialog form, if all fields are filled correctly, opens a new
        # dialog to select a file to save the new config file. If the new file is saved
        # exits with signal 1.
        pf8_config = self._config["peakfinder8_peak_detection"]
        pf8_config["adc_threshold"] = float(self._adc_threshold_le.text())
        pf8_config["minimum_snr"] = float(self._min_snr_le.text())
        pf8_config["min_pixel_count"] = int(self._min_pix_count_le.text())
        pf8_config["max_pixel_count"] = int(self._max_pix_count_le.text())
        pf8_config["local_bg_radius"] = int(self._local_bg_radius_le.text())
        pf8_config["min_res"] = int(self._min_res_le.text())
        pf8_config["max_res"] = int(self._max_res_le.text())
        self._config["crystallography"]["min_num_peaks_for_hit"] = int(
            self._min_peaks_for_hit.text()
        )
        filename: str = QtWidgets.QFileDialog.getSaveFileName(
            self, "Save new config file", str(self._new_config_file), filter="*.yaml"
        )[0]
        if filename:
            fh: TextIO
            with open(filename, "w") as fh:
                logger.info(f"Saving new config file: {filename}")
                self._yaml.dump(self._config, fh)

            self._new_config_file = pathlib.Path(filename)
            self.done(1)

    def _cancel(self) -> None:
        # Exits with signal 0.
        self.done(0)

    def _check_config(self) -> None:
        # Checks that all fields in the form are filled. If not, disables "OK" button.
        if "" in (
            self._adc_threshold_le.text(),
            self._min_snr_le.text(),
            self._min_pix_count_le.text(),
            self._max_pix_count_le.text(),
            self._local_bg_radius_le.text(),
            self._min_res_le.text(),
            self._max_res_le.text(),
            self._min_peaks_for_hit.text(),
        ):
            self._button_box.buttons()[0].setEnabled(False)
        else:
            self._button_box.buttons()[0].setEnabled(True)

    def get_new_config_file(self) -> pathlib.Path:
        """
        Get new config file.

        This function is called when the dialog exits with signal 1, which means that
        the new config file was saved.

        Returns:

            Path to the new config file.
        """
        return self._new_config_file

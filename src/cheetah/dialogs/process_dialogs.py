import pathlib

from PyQt5 import QtWidgets  # type: ignore
from typing import Any, Union, Dict
try:
    from typing import Literal
except:
    from typing_extensions import Literal

from cheetah.dialogs.generic_dialogs import PathDoesNotExistDialog
from cheetah.experiment import TypeExperimentConfig, TypeProcessingConfig


class RunProcessingDialog(QtWidgets.QDialog):  # type: ignore
    """
    See documentation of the `__init__` function
    """

    def __init__(
        self,
        last_config: TypeProcessingConfig,
        parent: Any = None,
    ) -> None:
        """ """
        super(RunProcessingDialog, self).__init__(parent)
        self.setWindowTitle("Run Cheetah")
        self.resize(800, 250)
        layout: Any = QtWidgets.QVBoxLayout(self)

        self._form: Any = QtWidgets.QGroupBox()
        form_layout: Any = QtWidgets.QFormLayout()

        self._template_le: Any = QtWidgets.QLineEdit()
        self._template_le.setText(last_config["config_template"])
        self._template_le.textChanged.connect(self._check_config)
        self._template_button: Any = QtWidgets.QPushButton("Browse")
        self._template_button.clicked.connect(self._select_config_template)
        template_layout: Any = QtWidgets.QHBoxLayout()
        template_layout.addWidget(self._template_le)
        template_layout.addWidget(self._template_button)
        form_layout.addRow("Cheetah config file*: ", template_layout)

        self._tag_le: Any = QtWidgets.QLineEdit()
        self._tag_le.setText(last_config["tag"])
        form_layout.addRow("Dataset name: ", self._tag_le)

        self._geometry_le: Any = QtWidgets.QLineEdit()
        self._geometry_le.setText(last_config["geometry"])
        self._geometry_le.textChanged.connect(self._check_config)
        self._geometry_button: Any = QtWidgets.QPushButton("Browse")
        self._geometry_button.clicked.connect(self._select_geometry_file)
        geometry_layout: Any = QtWidgets.QHBoxLayout()
        geometry_layout.addWidget(self._geometry_le)
        geometry_layout.addWidget(self._geometry_button)
        form_layout.addRow("Geometry file*: ", geometry_layout)

        self._mask_le: Any = QtWidgets.QLineEdit()
        self._mask_le.setText(last_config["mask"])
        self._mask_button: Any = QtWidgets.QPushButton("Browse")
        self._mask_button.clicked.connect(self._select_mask_file)
        mask_layout: Any = QtWidgets.QHBoxLayout()
        mask_layout.addWidget(self._mask_le)
        mask_layout.addWidget(self._mask_button)
        form_layout.addRow("Cheetah config file: ", mask_layout)

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
        self._check_config()
        template_file: pathlib.Path = pathlib.Path(self._config["config_template"])
        if not template_file.is_file():
            PathDoesNotExistDialog(
                template_file.parent, template_file.name, self
            ).exec()
            return
        geometry_file: pathlib.Path = pathlib.Path(self._config["geometry"])
        if not geometry_file.is_file():
            PathDoesNotExistDialog(
                geometry_file.parent, geometry_file.name, self
            ).exec()
            return
        if self._config["mask"]:
            mask_file: pathlib.Path = pathlib.Path(self._config["mask"])
            if not mask_file.is_file():
                PathDoesNotExistDialog(mask_file.parent, mask_file.name, self).exec()
                return
        self.done(1)

    def _cancel(self) -> None:
        self.done(0)

    def _check_config(self) -> None:
        self._config: TypeProcessingConfig = {
            "config_template": self._template_le.text(),
            "tag": self._tag_le.text(),
            "geometry": self._geometry_le.text(),
            "mask": self._mask_le.text(),
        }
        if not self._config["config_template"] or not self._config["geometry"]:
            self._button_box.buttons()[0].setEnabled(False)
        else:
            self._button_box.buttons()[0].setEnabled(True)

    def _select_config_template(self) -> None:
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

    def get_config(
        self,
    ) -> TypeProcessingConfig:
        """ """
        return self._config

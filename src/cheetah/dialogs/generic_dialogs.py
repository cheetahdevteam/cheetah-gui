import pathlib

from PyQt5 import QtWidgets  # type: ignore
from typing import Any, Union


class PathDoesNotExistDialog(QtWidgets.QDialog):  # type: ignore
    """
    See documentation of the `__init__` function
    """

    def __init__(
        self, path: pathlib.Path, filename: Union[str, None] = None, parent: Any = None
    ) -> None:
        """ """
        super(PathDoesNotExistDialog, self).__init__(parent)
        self.setWindowTitle("Uh oh")

        self._copy_path_button: Any = QtWidgets.QPushButton("Copy path", self)
        self._copy_path_button.clicked.connect(self._copy_path)
        self._path_copied_label: Any = QtWidgets.QLabel("")
        self._ok_button: Any = QtWidgets.QDialogButtonBox(QtWidgets.QDialogButtonBox.Ok)
        self._ok_button.accepted.connect(self.accept)

        buttons_layout: Any = QtWidgets.QHBoxLayout()
        buttons_layout.addWidget(self._copy_path_button)
        buttons_layout.addWidget(self._path_copied_label)
        buttons_layout.addWidget(self._ok_button)

        if filename:
            message: str = (
                f"There does not seem to be a {filename} file in this directory.\n"
                "Please check."
            )
            self._path: pathlib.Path = path / filename
        else:
            message = (
                "It looks like that directory does not exist any more.\n"
                "It may have been moved or deleted. Plesae check it still exists."
            )
            self._path = path

        self._message_label = QtWidgets.QLabel(message)

        layout = QtWidgets.QVBoxLayout()
        layout.addWidget(self._message_label)
        layout.addLayout(buttons_layout)
        self.setLayout(layout)

    def _copy_path(self) -> None:
        QtWidgets.QApplication.clipboard().setText(str(self._path))
        self._path_copied_label.setText("Path copied to clipboard")

"""
Generic dialogs.

This module contains generic dialogs which can be triggered by various events in 
Cheetah GUI programs.
"""

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
        """
        Not existing path dialog.

        This dialog shows a message when Cheetah encounters either a file or a
        directory path which doesn't exist. It also allows to copy the not existing
        path to clipboard.

        Arguments:

            path: The path which doesn't exist or the path of the parent directory of
                the file which doesn't exist.

            filename: The name of the file which doesn't exist. If the value is None,
                the message will say that the `path` directory does not exist. Defaults
                to None.

            parent: Parent QWidget. Defaults to None.
        """
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
                f"File {path / filename} doesn't exist or isn't a file.\n"
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
        # Copies path to clipboard when "Copy path" button is clicked.
        QtWidgets.QApplication.clipboard().setText(str(self._path))
        self._path_copied_label.setText("Path copied to clipboard")

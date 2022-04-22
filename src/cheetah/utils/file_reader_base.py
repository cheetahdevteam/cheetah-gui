"""
File reader base.
"""
from abc import ABCMeta, abstractmethod

from typing import Any, List, Dict, Set, TextIO, Union, cast
from PyQt5 import QtCore, QtWidgets  # type: ignore


class _QtMetaclass(type(QtCore.QObject), ABCMeta):  # type: ignore
    # This metaclass is used internally to resolve an issue with classes that inherit
    # from Qt and non-Qt classes at the same time.
    pass


class FileReader(QtCore.QObject, metaclass=_QtMetaclass):  # type: ignore
    """
    See documentation of the `__init__` function.
    """

    output: Any = QtCore.pyqtSignal(dict)
    """ 
    Qt signal emitted periodically to transmit accumulated data.    
    """

    def __init__(
        self,
        filenames: List[str],
        parameters: Dict[str, Any] = {},
        output_emit_interval: int = 2000,
        sleep_timeout: int = 0,
    ):
        """
        Base file reader class for Cheetah GUI programs.

        This class is designed to be run in a separate Qt thread. It continuously reads
        from one or several text files while they are being written by other programs,
        emitting accumulated information with a defined period. When it reaches the end
        of all input files it can optionally wait a certain time before attempting to
        continue reading. The logic of how the text data is processed and what
        information is transmitted should be implemented by the derived class.

        This class is a base class, each derived class should provide it's specific
        implementation of the abstract methods
        [_process_line][cheetah.utils.file_reader_base.FileReader._process_line] and
        [_prepare_data][cheetah.utils.file_reader_base.FileReader._prepare_data].

        Arguments:

            filenames: A list of input file names.

            parameters: A dictionary containing parameters specific for each derived
                class.

            output_emit_interval: The interval, in milliseconds, between each attempt
                to transmit accumulated data via the
                [output][cheetah.utils.file_reader_base.FileReader.output] signal.
                Defaults to 2000.

            sleep_timeout: The wait time, in milliseconds, after reaching the end of
                all input files before attempting to continue reading. Defaults to 0.
        """
        super(FileReader, self).__init__()
        self._filenames: List[str] = []
        self._files: List[TextIO] = []
        for filename in filenames:
            self._files.append(open(filename, "r"))
            self._filenames.append(filename)
        self._current: int = 0
        self._parameters: Dict[str, Any] = parameters
        self._output_emit_interval: int = output_emit_interval
        self._sleep_timeout: int = sleep_timeout

    def start(self) -> None:
        """
        Start reading files.

        This function creates emit and sleep timers and starts reading input files. The
        start of the Qt thread where the FileReader will be running should connect to
        this function.
        """
        self._emit_timer: Any = QtCore.QTimer()
        self._emit_timer.timeout.connect(self._emit_data)

        self._sleep_timer: Any = QtCore.QTimer()
        self._sleep_timer.timeout.connect(self._stop_sleeping)

        self._read_files()

    def stop(self) -> None:
        """
        Stop reading files.

        This function stops all timers telling the FileReader to stop reading files.
        This function should be called before the main application is closed and the
        thread is killed for clean exit.
        """
        self._emit_timer.stop()
        self._sleep_timer.stop()

    def _read_files(self) -> None:
        # This function starts the emit timer and reads the lines from the input files
        # in a loop passing them to _process_line() function. The loop is interrupted
        # when emit timer times out or when the end of all input files is reached.
        self._emit_timer.start(self._output_emit_interval)
        finished_files: Set[int] = set()
        while self._emit_timer.isActive() and not self._sleep_timer.isActive():
            QtWidgets.QApplication.processEvents()
            line = self._files[self._current].readline()
            if not line:
                finished_files.add(self._current)
                if len(finished_files) == len(self._files):
                    if self._sleep_timeout > 0:
                        self._sleep_timer.start(self._sleep_timeout)
                self._current += 1
                if self._current == len(self._files):
                    self._current = 0
                continue
            else:
                finished_files.discard(self._current)
            self._process_line(line)

    def _stop_sleeping(self) -> None:
        # This function is called when sleep timer times out.
        self._sleep_timer.stop()
        self._read_files()

    def _emit_data(self) -> None:
        # This function is called when emit timer times out. It calls _prepare_output()
        # function and emits the output if it's not None.
        self._emit_timer.stop()
        data: Union[None, Dict[str, Any]] = self._prepare_output()
        if data is not None:
            self.output.emit(data)
        if not self._sleep_timer.isActive():
            self._emit_timer.start(self._output_emit_interval)

    @abstractmethod
    def _process_line(self, line: str) -> None:
        # Process a line from the input file.
        pass

    @abstractmethod
    def _prepare_output(self) -> Union[None, Dict[str, Any]]:
        # Prepare output dictionary from accumulated data.
        pass

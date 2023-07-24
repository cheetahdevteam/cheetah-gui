"""
Logging utils.
"""
import logging
import pathlib
import subprocess
from threading import Thread
from typing import Any, Callable, Dict, TextIO

from PyQt5 import QtCore  # type: ignore

logging_config: Dict[str, Any] = {
    "version": 1,
    "formatters": {
        "simple": {"()": "cheetah.utils.logging.SimpleFormatter"},
        "gui": {"()": "cheetah.utils.logging.GuiFormatter"},
        "full": {
            "format": "[%(asctime)s.%(msecs)03d] %(name)s - %(levelname)s - %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console_gui": {
            "class": "logging.StreamHandler",
            "formatter": "gui",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        },
        "console_simple": {
            "class": "logging.StreamHandler",
            "formatter": "simple",
            "level": "INFO",
            "stream": "ext://sys.stdout",
        },
        "file": {
            "class": "logging.handlers.RotatingFileHandler",
            "formatter": "full",
            "level": "INFO",
            "filename": pathlib.Path.home() / ".cheetah/logs/gui.log",
            "maxBytes": 10485760,
            "backupCount": 20,
        },
    },
    "loggers": {
        "cheetah": {
            "level": "INFO",
            "handlers": [
                "file",
            ],
            "propagate": False,
        },
        "cheetah_viewer": {
            "level": "INFO",
            "handlers": ["console_simple"],
            "propagate": False,
        },
        "cheetah.frame_retrieval": {
            "level": "INFO",
            "handlers": ["console_simple"],
            "propagate": False,
        },
    },
    "root": {"level": "INFO", "handlers": ["console_gui"]},
}
"""
Logging configuration dictionary.
"""


def log_subprocess_run_output(
    output: subprocess.CompletedProcess,
    logger: logging.Logger,
    only_errors: bool = False,
) -> None:
    """
    Log the output of a subprocess run.

    Arguments:

        output: The output of the subprocess run.

        logger: The logging.Logger instance to use for logging the output.
    """
    line: str
    for line in output.stderr.decode().splitlines():
        if line:
            logger.error(line)
    if not only_errors:
        for line in output.stdout.decode().splitlines():
            if line:
                logger.info(line)


class LoggingPopen(subprocess.Popen):
    """
    See documentation of the `__init__` function.
    """

    @staticmethod
    def _read_lines(pipe: TextIO, log: Callable[[str], None]) -> None:
        """
        Read lines from pipe and log them.

        Arguments:

            pipe: The pipe to read from.

            log: The logging function to use.
        """
        with pipe:
            line: str
            for line in pipe:
                log(line.strip())

    def __init__(self, logger: logging.Logger, *args, **kwargs) -> None:
        """
        This class is a wrapper around the `subprocess.Popen` class. It starts a new
        thread for each of the `stdout` and `stderr` pipes of the process and logs the
        `stdout` and `stderr` lines using the `logging.info` and `logging.error`
        functions.

        Arguments:

            logger: The logging.Logger instance to use for logging the `stdout` and
                `stderr` lines.

            *args: The arguments to pass to the `subprocess.Popen` constructor.

            **kwargs: The keyword arguments to pass to the `subprocess.Popen`
                constructor. `stdout` and `stderr` keyword arguments are overwritten
                with `subprocess.PIPE`, `bufsize` keyword argument is overwritten with
                `1` and `universal_newlines` keyword argument is overwritten with True.
        """
        kwargs["stdout"] = subprocess.PIPE
        kwargs["stderr"] = subprocess.PIPE
        kwargs["universal_newlines"] = True
        kwargs["bufsize"] = 1
        super(LoggingPopen, self).__init__(*args, **kwargs)

        Thread(target=self._read_lines, args=[self.stdout, logger.info]).start()
        Thread(target=self._read_lines, args=[self.stderr, logger.error]).start()


class GuiFormatter(logging.Formatter):
    """
    See documentation of the `__init__` function.
    """

    info_fmt: str = "[%(asctime)s.%(msecs)03d] %(name)s - %(message)s"
    """
    The format string for the `INFO` log level.
    """

    fmt: str = "[%(asctime)s.%(msecs)03d] %(name)s - %(levelname)s - %(message)s"
    """
    The format string for the `DEBUG`, `ERROR` and `WARNING` log levels.
    """

    def __init__(self) -> None:
        """
        This class is a wrapper around the `logging.Formatter` class. It formats the log
        records in a way that is suitable for the GUI.
        """
        datefmt: str = "%H:%M:%S"
        super().__init__(fmt=GuiFormatter.info_fmt, datefmt=datefmt)

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record.

        Arguments:

            record: The log record.

        Returns:

            The formatted log record.
        """

        if record.levelno == logging.INFO:
            self._style._fmt = GuiFormatter.info_fmt
        else:
            self._style._fmt = GuiFormatter.fmt
        result = logging.Formatter.format(self, record)
        return result


class SimpleFormatter(logging.Formatter):
    """
    See documentation of the `__init__` function.
    """

    info_fmt: str = "%(message)s"
    """
    The format string for the `INFO` log level.
    """

    fmt: str = "%(levelname)s - %(message)s"
    """
    The format string for the `DEBUG`, `ERROR` and `WARNING` log levels.
    """

    def __init__(self) -> None:
        """
        This class is a wrapper around the `logging.Formatter` class. It formats the log
        records only with the message and the log level. If the log level is `INFO` then
        only the message is logged.
        """
        super().__init__(fmt=SimpleFormatter.info_fmt)

    def format(self, record: logging.LogRecord) -> str:
        """
        Format the log record.

        Arguments:

            record: The log record.

        Returns:

            The formatted log record.
        """

        if record.levelno == logging.INFO:
            self._style._fmt = SimpleFormatter.info_fmt
        else:
            self._style._fmt = SimpleFormatter.fmt
        result = logging.Formatter.format(self, record)
        return result


class QtHandler(QtCore.QObject, logging.Handler):
    """
    See documentation of the `__init__` function.
    """

    new_record: Any = QtCore.pyqtSignal(object)
    """
    Qt signal emitted when a new log record is received.
    """

    def __init__(self, parent: Any = None) -> None:
        """
        This class is a wrapper around the `logging.Handler` class. It emits a Qt signal
        when a new log record is received.

        Arguments:

                parent: Parent Qt Object. Defaults to None.
        """
        super().__init__(parent)
        super(logging.Handler).__init__()
        formatter: logging.Formatter = GuiFormatter()
        self.setFormatter(formatter)

    def emit(self, record: logging.LogRecord) -> None:
        """
        Emit a Qt signal when a new log record is received.

        Arguments:

            record: The log record.
        """
        msg: str = self.format(record)
        self.new_record.emit(msg)

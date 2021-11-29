import pathlib
import shutil

from typing import Callable
from cheetah.crawlers import facilities


class CheetahProcess:
    """
    See documentation of the `__init__` function.
    """

    def __init__(
        self,
        facility: str,
        experiment_id: str,
        process_template: pathlib.Path,
        raw_directory: pathlib.Path,
        proc_directory: pathlib.Path,
    ) -> None:
        """ """
        self._facility: str = facility
        self._experiment_id: str = experiment_id
        self._process_template: pathlib.Path = process_template
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._prepare_om_source: Callable[
            [str, str, pathlib.Path, pathlib.Path], str
        ] = facilities[self._facility]["prepare_om_source"]

    def process_run(self, run_id: str, om_template: pathlib.Path, tag: str) -> None:
        """ """
        proc_id: str = self._raw_id_to_proc_id(run_id)
        output_directory_name: str = f"{proc_id}-{tag}"
        output_directory: pathlib.Path = self._proc_directory / output_directory_name

        if output_directory.is_dir():
            print(
                f"Moving to existing data directory {output_directory}\n"
                f"Deleting previous files"
            )
            shutil.rmtree(output_directory)
        else:
            print(f"Creating hdf5 data directory {output_directory}")
        output_directory.mkdir()

        print(f"Copying configuration file: {om_template}")
        om_config: pathlib.Path = output_directory / "monitor.yaml"
        shutil.copyfile(om_template, om_config)

        # TODO: fill template

        process_script: pathlib.Path = output_directory / "process.sh"
        shutil.copyfile(self._process_template, process_script)
        om_source: str = self._prepare_om_source(
            run_id, self._experiment_id, self._raw_directory, output_directory
        )

        # TODO: fill process script

    def _raw_id_to_proc_id(self, raw_id: str) -> str:
        """ """
        return raw_id.replace("-", "_")

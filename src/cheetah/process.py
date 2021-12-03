import click  # type: ignore
import jinja2
import pathlib
import shutil
import stat
import subprocess

from typing import Callable, TextIO, Union, TypedDict, Literal

from cheetah.crawlers import facilities


class TypeOmConfigTemplateData(TypedDict):
    psana_calib_dir: pathlib.Path
    output_dir: pathlib.Path
    experiment_id: str
    run_id: str
    geometry_file: pathlib.Path
    mask_file: Union[pathlib.Path, Literal["null"]]


class TypeProcessScriptTemplateData(TypedDict):
    queue: str
    job_name: str
    n_processes: int
    om_source: str
    om_config: pathlib.Path


class TypeProcessingConfig(TypedDict):
    config_template: str
    tag: str
    geometry: str
    mask: str


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
        self._process_template_file: pathlib.Path = process_template
        fh: TextIO
        with open(self._process_template_file) as fh:
            self._process_template: jinja2.Template = jinja2.Template(fh.read())
        self._raw_directory: pathlib.Path = raw_directory
        self._proc_directory: pathlib.Path = proc_directory
        self._prepare_om_source: Callable[
            [str, str, pathlib.Path, pathlib.Path], str
        ] = facilities[self._facility]["prepare_om_source"]

    def _raw_id_to_proc_id(self, raw_id: str) -> str:
        """ """
        return raw_id.replace("-", "_")

    def _write_process_config_file(
        self, output_directory: pathlib.Path, config: TypeProcessingConfig
    ) -> None:
        fh: TextIO
        with open(output_directory / "process_config.txt", "w") as fh:
            for key, value in config.items():
                fh.write(f"{key}: {value}\n")

    def _write_status_file(self, output_directory: pathlib.Path) -> None:
        fh: TextIO
        with open(output_directory / "status.txt", "w") as fh:
            fh.write("# Cheetah status\n")
            fh.write("Status: Submitted\n")

    def process_run(
        self,
        run_id: str,
        config: TypeProcessingConfig,
        queue: Union[str, None] = None,
        n_processes: Union[int, None] = None,
    ) -> None:
        """ """
        om_config_template_file: pathlib.Path = pathlib.Path(config["config_template"])
        tag: str = config["tag"]
        geometry_file: pathlib.Path = pathlib.Path(config["geometry"])
        if config["mask"]:
            mask_file: Union[pathlib.Path, Literal["null"]] = pathlib.Path(
                config["mask"]
            )
        else:
            mask_file = "null"

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

        print(f"Copying configuration file: {om_config_template_file}")
        om_config_file: pathlib.Path = output_directory / "monitor.yaml"

        fh: TextIO
        with open(om_config_template_file) as fh:
            om_config_template: jinja2.Template = jinja2.Template(fh.read())

        om_config_data: TypeOmConfigTemplateData = {
            "psana_calib_dir": self._raw_directory.parent / "calib",
            "output_dir": output_directory,
            "experiment_id": self._experiment_id,
            "run_id": proc_id,
            "geometry_file": geometry_file,
            "mask_file": mask_file,
        }
        with open(om_config_file, "w") as fh:
            fh.write(om_config_template.render(om_config_data))

        process_script: pathlib.Path = output_directory / "process.sh"
        om_source: str = self._prepare_om_source(
            run_id, self._experiment_id, self._raw_directory, output_directory
        )

        if not queue:
            queue = facilities[self._facility]["guess_batch_queue"](self._raw_directory)
        if not n_processes:
            n_processes = 12
        process_script_data: TypeProcessScriptTemplateData = {
            "queue": queue,
            "job_name": output_directory_name,
            "n_processes": n_processes,
            "om_source": om_source,
            "om_config": om_config_file,
        }
        with open(process_script, "w") as fh:
            fh.write(self._process_template.render(process_script_data))

        process_script.chmod(process_script.stat().st_mode | stat.S_IEXEC)
        subprocess.run(f"{process_script}", cwd=output_directory)
        self._write_status_file(output_directory)
        self._write_process_config_file(output_directory, config)


@click.command()  # type: ignore
def main() -> None:
    """ """
    pass
    # TODO: make cheetah_process.py a standalone script


if __name__ == "__main__":
    main()

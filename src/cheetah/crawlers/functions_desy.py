"""
P11 functions.

This module contains functions required by Cheetah GUI at P11 beamline at PETRA III.
"""
import pathlib
import subprocess

from typing import Tuple, TextIO


def guess_batch_queue_desy(path: pathlib.Path) -> str:
    """
    Guess the appropriate batch queue to be used at DESY.

    This function guesses the name of the batch queue available for users at DESY
    facilities (PETRA III) to run data processing based on the experiment directory
    path. It returns "all", which is a partition available for all DESY users.

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        The name of the appropriate batch queue if it is possible to guess from the
        experiment directory path, otherwise an empty string.
    """
    return "all"


def guess_experiment_id_desy(path: pathlib.Path) -> str:
    """
    Guess the experiment ID at DESY.

    This function guesses the experiment ID based on the experiment directory path.

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        Experiment ID if it is possible to guess from the experiment directory path,
        otherwise an empty string.
    """
    parts: Tuple[str, ...] = path.parts
    if parts[1] == "asap3" and len(parts) > 7:
        return parts[7]
    elif (
        pathlib.Path("/gpfs/cfel/group/cxi/scratch/data/") in path.parents
        and len(parts) > 8
    ):
        return parts[8]
    else:
        return ""


def guess_raw_directory_desy(path: pathlib.Path) -> pathlib.Path:
    """
    Guess the raw data directory path at DESY.

    This function guesses the path to the raw data based on the experiment directory
    path.

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        Possible raw data directory path.
    """
    parts: Tuple[str, ...] = path.parts
    index: int = len(parts) - 3
    if parts[1] == "asap3" and len(parts) > 7:
        index = 6
    elif (
        pathlib.Path("/gpfs/cfel/group/cxi/scratch/data/") in path.parents
        and len(parts) > 8
    ):
        index = 7
    return pathlib.Path(*parts[: index + 2]) / "raw"


def prepare_om_source_p11_eiger(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    """
    Prepare OM data source for the processing of Eiger 16M data collected at P11
    beamline at PETRA III.

    The OM data source string for the data retrieval from Eiger 16M files is a text
    file containing the list of Eiger HDF5 files. This function writes the list of
    names of all data files belonging to a given run to the files.lst file in the
    `run_proc_directory`.

    Arguments:

        run_id: Run ID of the raw data.

        experiment_id: Experiment ID.

        raw_directory: The raw data directory path of the experiment.

        run_proc_directory: The processed data directory path of the run.

    Returns:

        OM data source string.
    """
    data_directory: pathlib.Path = (raw_directory / run_id).parent
    filename_prefix: str = run_id.split("/")[-1]
    fh: TextIO
    with open(run_proc_directory / "files.lst", "w") as fh:
        fh.writelines(
            (
                f"{data_directory/filename}\n"
                for filename in sorted(
                    data_directory.glob(f"{filename_prefix}_data_*.h5")
                )
            )
        )

    return str(run_proc_directory / "files.lst")


def prepare_om_source_biocars_mccd(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    """
    Prepare OM data source for the processing of Rayonix MX340-HS data collected at
    BioCARS beamline at APS.

    The OM data source string for the data retrieval from Rayonix 16M files is a text
    file containing the list of Rayonix mccd files. This function writes the list of
    names of all data files belonging to a given run to the files.lst file in the
    `run_proc_directory`.

    Arguments:

        run_id: Run ID of the raw data.

        experiment_id: Experiment ID.

        raw_directory: The raw data directory path of the experiment.

        run_proc_directory: The processed data directory path of the run.

    Returns:

        OM data source string.
    """
    data_directory: pathlib.Path = raw_directory / run_id
    output_filename: pathlib.Path = run_proc_directory / "files.lst"
    command = (
        f'find {data_directory} -maxdepth 1 -name "*.mccd" | sort > {output_filename}'
    )
    print(
        subprocess.run(command, shell=True, capture_output=True).stderr.decode("utf-8")
    )

    return str(output_filename)


def prepare_om_source_jungfrau1M(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    """
    Prepare OM data source for the processing of Junfrau 1M data.

    The OM data source string for the data retrieval from Jungfrau 1M files is a text
    file containing the list of 'master' HDF5 files. This function writes the list of
    names of files belonging to a given run to the files.lst file in the
    `run_proc_directory`.

    Arguments:

        run_id: Run ID of the raw data.

        experiment_id: Experiment ID.

        raw_directory: The raw data directory path of the experiment.

        run_proc_directory: The processed data directory path of the run.

    Returns:

        OM data source string.
    """
    data_directory: pathlib.Path = (raw_directory / run_id).parent
    filename_prefix: str = run_id.split("/")[-1]
    fh: TextIO
    with open(run_proc_directory / "files.lst", "w") as fh:
        fh.writelines(
            (
                f"{data_directory/filename}\n"
                for filename in sorted(
                    data_directory.glob(f"{filename_prefix}_master_*.h5")
                )
            )
        )

    return str(run_proc_directory / "files.lst")


def prepare_om_source_p09_lambda(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    """
    Prepare OM data source for the processing of Lambda 1.5M data collected at P09
    beamline at PETRA III.

    The OM data source string for the data retrieval from Lambda 1.5M files is a text
    file containing the list of Lambda HDF5 files written by the first detector module.
    This function writes the list of names of all m01.nxs files belonging to a given
    run to the files.lst file in the `run_proc_directory`.

    Arguments:

        run_id: Run ID of the raw data.

        experiment_id: Experiment ID.

        raw_directory: The raw data directory path of the experiment.

        run_proc_directory: The processed data directory path of the run.

    Returns:

        OM data source string.
    """
    data_directory: pathlib.Path = raw_directory / run_id
    output_filename: pathlib.Path = run_proc_directory / "files.lst"
    command = (
        f'find {data_directory} -maxdepth 1 -name "*m01*.nxs" | sort > '
        f"{output_filename}"
    )
    print(
        subprocess.run(command, shell=True, capture_output=True).stderr.decode("utf-8")
    )

    return str(output_filename)

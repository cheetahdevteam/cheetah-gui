"""
LCLS functions.

This module contains functions required by Cheetah GUI at LCLS.
"""
import pathlib

from typing import Tuple


def guess_batch_queue_lcls(path: pathlib.Path) -> str:
    """
    Guess the appropriate batch queue to be used at LCLS.

    This function guesses the name of the batch queue available for users at LCLS to
    run data processing based on the experiment directory path. It returns "psanaq" on
    psana, "anaq" on ffb and "shared" on sdf. For more information about available
    computing resources at LCLS please see SLAC
    [confluence][https://confluence.slac.stanford.edu/pages/viewpage.action?pageId=92183280].

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        The name of the appropriate batch queue if it is possible to guess from the
        experiment directory path, otherwise an empty string.
    """
    ffb_root: pathlib.Path = pathlib.Path("/cds/data/drpsrcf/")
    psana_root: pathlib.Path = pathlib.Path("/cds/data/psdm/")
    psana_old_root: pathlib.Path = pathlib.Path("/reg/d/psdm/")
    sdf_root: pathlib.Path = pathlib.Path("/sdf/group/lcls/ds/data/")
    if ffb_root in path.parents:
        return "anaq"
    elif psana_root in path.parents or psana_old_root in path.parents:
        return "psanaq"
    elif sdf_root in path.parents:
        return "shared"
    else:
        return ""


def guess_experiment_id_lcls(path: pathlib.Path) -> str:
    """
    Guess the experiment ID at LCLS.

    This function guesses the experiment ID based on the experiment directory path.

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        Experiment ID if it is possible to guess from the experiment directory path,
        otherwise an empty string.
    """
    parts: Tuple[str, ...] = path.parts
    index: int = -1
    for instrument in ("CXI", "MFX"):
        if instrument in parts:
            index = parts.index(instrument)
        elif instrument.lower() in parts:
            index = parts.index(instrument.lower())
    if index < 0 or index == len(parts) - 1:
        return ""
    else:
        return parts[index + 1]


def guess_raw_directory_lcls(path: pathlib.Path) -> pathlib.Path:
    """
    Guess the raw data directory path at LCLS.

    This function guesses the path to the raw data based on the experiment directory
    path.

    Arguments:

        path: The full path to the experiment directory.

    Returns:

        Possible raw data directory path.
    """
    parts: Tuple[str, ...] = path.parts
    index: int = len(parts) - 3
    for instrument in ("CXI", "MFX"):
        if instrument in parts:
            index = parts.index(instrument)
        elif instrument.lower() in parts:
            index = parts.index(instrument.lower())
    return pathlib.Path(*parts[: index + 2]) / "xtc"


def prepare_om_source_lcls(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    """
    Prepare OM data source for the data processing at LCLS.

    The OM data source string for data retrieval at LCLS is the same as psana data
    source. This function creates psana data source string from the experiment ID, run
    number and raw data directory.

    Arguments:

        run_id: Run ID of the raw data.

        experiment_id: Experiment ID.

        raw_directory: The raw data directory path of the experiment.

        run_proc_directory: The processed data directory path of the run.

    Returns:

        OM data source string.
    """
    run_number: int = int(run_id[1:])
    return f"exp={experiment_id}:run={run_number}:dir={raw_directory}"

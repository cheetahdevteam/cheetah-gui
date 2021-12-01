import pathlib

from typing import Tuple


def guess_batch_queue_lcls(path: pathlib.Path) -> str:
    """ """
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
    """ """
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
    """ """
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
    """ """
    run_number: int = int(run_id[1:])
    return f"exp={experiment_id}:run={run_number}:dir={raw_directory}"

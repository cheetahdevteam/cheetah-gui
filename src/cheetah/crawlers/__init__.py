import pathlib

from typing import Any, Dict, Callable, TypedDict, Type
from cheetah.crawlers.lcls import LclsCrawler
from cheetah.crawlers.base import Crawler


def guess_raw_directory_lcls(path: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(*path.parts[:6]) / "xtc"


def prepare_om_source_lcls(
    run_id: str,
    experiment_id: str,
    raw_directory: pathlib.Path,
    run_proc_directory: pathlib.Path,
) -> str:
    run_number: int = int(run_id[1:])
    return f"exp={experiment_id}:run={run_number}:dir={raw_directory}"


class TypeFacility(TypedDict):
    instruments: Dict[str, Any]
    guess_raw_directory: Callable[[pathlib.Path], pathlib.Path]
    prepare_om_source: Callable[[str, str, pathlib.Path, pathlib.Path], str]
    crawler: Type[Crawler]


facilities: Dict[str, TypeFacility] = {
    "LCLS": {
        "instruments": {
            "MFX": {
                "detectors": {
                    "epix10k2M": {
                        "resources": [
                            "epix10k2M.geom",
                            "mask_epix10k2M.h5",
                        ]
                    },
                    "cspad": {
                        "resources": [
                            "cspad.geom",
                            "mask_cspad.h5",
                        ]
                    },
                },
            },
            "CXI": {
                "detectors": {
                    "jungfrau4M": {
                        "resources": [
                            "jungfrau4M.geom",
                            "mask_jungfrau4M.h5",
                        ]
                    },
                    "cspad": {
                        "resources": [
                            "cspad.geom",
                            "mask_cspad.h5",
                        ]
                    },
                },
            },
        },
        "guess_raw_directory": guess_raw_directory_lcls,
        "prepare_om_source": prepare_om_source_lcls,
        "crawler": LclsCrawler,
    },
}

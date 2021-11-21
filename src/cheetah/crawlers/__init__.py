import pathlib
from sys import path
from typing import Any, Dict, Callable, TypedDict
from cheetah.crawlers.lcls_crawler import LclsCrawler


def guess_raw_directory_lcls(path: pathlib.Path) -> pathlib.Path:
    return pathlib.Path(*path.parts[:6]) / "xtc"


class Facility(TypedDict):
    instruments: Dict[str, Any]
    guess_raw_directory: Callable[[pathlib.Path], pathlib.Path]
    crawler: Any


facilities: Dict[str, Facility] = {
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
        "crawler": LclsCrawler,
    },
}

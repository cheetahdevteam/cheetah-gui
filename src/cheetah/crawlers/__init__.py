import pathlib

from typing import Any, Dict, Callable, Type

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.crawlers.base import Crawler
from cheetah.crawlers.crawler_lcls import LclsCrawler
from cheetah.crawlers.functions_lcls import (
    guess_batch_queue_lcls,
    guess_experiment_id_lcls,
    guess_raw_directory_lcls,
    prepare_om_source_lcls,
)


class TypeFacility(TypedDict):
    instruments: Dict[str, Any]
    guess_raw_directory: Callable[[pathlib.Path], pathlib.Path]
    guess_experiment_id: Callable[[pathlib.Path], str]
    guess_batch_queue: Callable[[pathlib.Path], str]
    prepare_om_source: Callable[[str, str, pathlib.Path, pathlib.Path], str]
    crawler: Type[Crawler]


facilities: Dict[str, TypeFacility] = {
    "LCLS": {
        "instruments": {
            "MFX": {
                "detectors": {
                    "epix10k2M": {
                        "calib_resources": {
                            "geometry": "epix10k2M.geom",
                            "mask": "mask_epix10k2M.h5",
                        },
                        "om_config_template": "mfx_epix_template.yaml",
                        "process_template": "slurm_template.sh",
                    },
                    "cspad": {
                        "calib_resources": {
                            "geometry": "cspad.geom",
                            "mask": "mask_cspad.h5",
                        },
                        "om_config_template": "mfx_cspad_template.yaml",
                        "process_template": "slurm_template.sh",
                    },
                },
            },
            "CXI": {
                "detectors": {
                    "jungfrau4M": {
                        "calib_resources": {
                            "geometry": "jungfrau4M.geom",
                            "mask": "mask_jungfrau4M.h5",
                        },
                        "om_config_template": "cxi_jungfrau_template.yaml",
                        "process_template": "slurm_template.sh",
                    },
                    "cspad": {
                        "calib_resources": {
                            "geometry": "cspad.geom",
                            "mask": "mask_cspad.h5",
                        },
                        "om_config_template": "cxi_cspad_template.yaml",
                        "process_template": "slurm_template.sh",
                    },
                },
            },
        },
        "guess_raw_directory": guess_raw_directory_lcls,
        "guess_experiment_id": guess_experiment_id_lcls,
        "prepare_om_source": prepare_om_source_lcls,
        "guess_batch_queue": guess_batch_queue_lcls,
        "crawler": LclsCrawler,
    },
}

"""
Cheetah Crawlers.

This package contains all facility-, instrument- and detector-dependent code in Cheetah 
GUI. Function and classes for different facilities are implemented in separate
modules in this package. 
"""

import pathlib
from typing import Callable, Dict, Optional, Type

try:
    from typing import TypedDict
except:
    from typing_extensions import TypedDict

from cheetah.crawlers.base import Crawler
from cheetah.crawlers.crawler_biocars import BioCarsMccdCrawler
from cheetah.crawlers.crawler_jungfrau import Jungfrau1MCrawler
from cheetah.crawlers.crawler_lcls import LclsCrawler
from cheetah.crawlers.crawler_p09 import P09LambdaCrawler
from cheetah.crawlers.crawler_p11 import P11EigerCrawler
from cheetah.crawlers.functions_desy import (
    guess_batch_queue_desy,
    guess_experiment_id_desy,
    guess_raw_directory_desy,
    prepare_om_source_biocars_mccd,
    prepare_om_source_jungfrau1M,
    prepare_om_source_p09_lambda,
    prepare_om_source_p11_eiger,
)
from cheetah.crawlers.functions_generic import kill_slurm_job
from cheetah.crawlers.functions_lcls import (
    guess_batch_queue_lcls,
    guess_experiment_id_lcls,
    guess_raw_directory_lcls,
    prepare_om_source_lcls,
)


class TypeDetectorInfo(TypedDict):
    """
    A dictionary storing information about resources associated with a certain detector.

    Resources associated with each detector include example geometry and mask files,
    OM config template and processing script template. These files are stored in
    cheetah_source/resources/ directory and copied to the Cheetah experiment directory
    when a new experiment is set up.

    Attributes:
        calib_resources: A dictionary storing information about calibration resources,
            such as detector geometry and mask. In this dictionary the keys are the
            names of the resources and the values are corresponding file names.

        om_config_template: The name of the OM config template file.

        process_template: The name of the process script template file.

        streaming_template: The name of the streaming script template file (optional).

        prepare_om_source: A function which prepares OM data source for data processing.

        crawler: Cheetah Crawler class for the facility.

    """

    calib_resources: Dict[str, str]
    om_config_template: str
    process_template: str
    streaming_template: Optional[str]
    prepare_om_source: Callable[[str, str, pathlib.Path, pathlib.Path], str]
    crawler: Type[Crawler]


class TypeInstrumentInfo(TypedDict):
    """
    A dictionary storing information about supported detectors and associated with them
    resources for a certain facility.

    Attributes:

        detectors: A dictionary storing information about supported detectors. In this
            dictionary the keys are detector names and the values are
            [TypeDetectorInfo][cheetah.crawlers.TypeDetectorInfo] dictionaries.
    """

    detectors: Dict[str, TypeDetectorInfo]


class TypeFacilityInfo(TypedDict):
    """
    A dictionary storing information about supported instruments and detectors for a
    certain facility as well as functions and classes associated with the facility.

    Attributes:

        instruments: A dictionary storing information about supported instruments. In
            this dictionary the keys are instrument names and the values are
            [TypeInstrumentInfo][cheetah.crawlers.TypeInstrumentInfo] dictionaries.

        guess_raw_directory: A function which guesses raw data directory based on the
            experiment directory path.

        guess_experiment_id: A function which guesses experiment ID based on the
            experiment directory path.

        guess_batch_queue: A function which guesses the appropriate batch queue name
            based on the experiment directory path.

        kill_processing_job: A function which kills OM processing job.
    """

    instruments: Dict[str, TypeInstrumentInfo]
    guess_raw_directory: Callable[[pathlib.Path], pathlib.Path]
    guess_experiment_id: Callable[[pathlib.Path], str]
    guess_batch_queue: Callable[[pathlib.Path], str]
    kill_processing_job: Callable[[str], str]


facilities: Dict[str, TypeFacilityInfo] = {
    "LCLS": {
        "instruments": {
            "MFX": {
                "detectors": {
                    "epix10k2M": {
                        "calib_resources": {
                            "geometry": "epix10k2M.geom",
                            "mask": "mask_epix10k2M.h5",
                            "psana_mask_script": "scripts/psana_mask.py",
                        },
                        "om_config_template": "mfx_epix_template.yaml",
                        "process_template": "lcls_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_lcls,
                        "crawler": LclsCrawler,
                    },
                    "rayonix": {
                        "calib_resources": {
                            "geometry": "rayonix.geom",
                            "mask": "mask_rayonix.h5",
                            "psana_mask_script": "scripts/psana_mask.py",
                        },
                        "om_config_template": "mfx_rayonix_template.yaml",
                        "process_template": "lcls_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_lcls,
                        "crawler": LclsCrawler,
                    },
                    "cspad": {
                        "calib_resources": {
                            "geometry": "cspad.geom",
                            "mask": "mask_cspad.h5",
                        },
                        "om_config_template": "mfx_cspad_template.yaml",
                        "process_template": "lcls_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_lcls,
                        "crawler": LclsCrawler,
                    },
                },
            },
            "CXI": {
                "detectors": {
                    "jungfrau4M": {
                        "calib_resources": {
                            "geometry": "jungfrau4M.geom",
                            "mask": "mask_jungfrau4M.h5",
                            "psana_mask_script": "scripts/psana_mask.py",
                        },
                        "om_config_template": "cxi_jungfrau_template.yaml",
                        "process_template": "lcls_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_lcls,
                        "crawler": LclsCrawler,
                    },
                    "cspad": {
                        "calib_resources": {
                            "geometry": "cspad.geom",
                            "mask": "mask_cspad.h5",
                        },
                        "om_config_template": "cxi_cspad_template.yaml",
                        "process_template": "lcls_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_lcls,
                        "crawler": LclsCrawler,
                    },
                },
            },
        },
        "guess_raw_directory": guess_raw_directory_lcls,
        "guess_experiment_id": guess_experiment_id_lcls,
        "guess_batch_queue": guess_batch_queue_lcls,
        "kill_processing_job": kill_slurm_job,
    },
    "DESY (PETRA III)": {
        "instruments": {
            "P09": {
                "detectors": {
                    "Lambda1M5": {
                        "calib_resources": {
                            "geometry": "lambda1M5.geom",
                            "mask": "mask_lambda1M5.h5",
                        },
                        "om_config_template": "p09_lambda_template.yaml",
                        "process_template": "desy_slurm_template.sh",
                        "streaming_template": "desy_slurm_streaming_template.sh",
                        "prepare_om_source": prepare_om_source_p09_lambda,
                        "crawler": P09LambdaCrawler,
                    }
                }
            },
            "P11": {
                "detectors": {
                    "Eiger16M": {
                        "calib_resources": {
                            "geometry": "eiger16M.geom",
                            "mask": "mask_eiger16M.h5",
                        },
                        "om_config_template": "p11_eiger_template.yaml",
                        "process_template": "desy_slurm_template.sh",
                        "streaming_template": "desy_slurm_streaming_template.sh",
                        "prepare_om_source": prepare_om_source_p11_eiger,
                        "crawler": P11EigerCrawler,
                    }
                }
            },
        },
        "guess_raw_directory": guess_raw_directory_desy,
        "guess_experiment_id": guess_experiment_id_desy,
        "guess_batch_queue": guess_batch_queue_desy,
        "kill_processing_job": kill_slurm_job,
    },
    "DESY (external beamtime)": {
        "instruments": {
            "APS/BioCARS": {
                "detectors": {
                    "RayonixMccd16M": {
                        "calib_resources": {
                            "geometry": "mccd16M.geom",
                            "mask": "mask_mccd16M.h5",
                        },
                        "om_config_template": "biocars_mccd_template.yaml",
                        "process_template": "desy_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_biocars_mccd,
                        "crawler": BioCarsMccdCrawler,
                    },
                    "Jungfrau1M": {
                        "calib_resources": {
                            "geometry": "jungfrau1M.geom",
                            "mask": "mask_jungfrau1M.h5",
                            "process_darks_script": "scripts/process_darks_jungfrau.py",
                        },
                        "om_config_template": "jungfrau1M_template.yaml",
                        "process_template": "desy_slurm_template.sh",
                        "streaming_template": None,
                        "prepare_om_source": prepare_om_source_jungfrau1M,
                        "crawler": Jungfrau1MCrawler,
                    },
                }
            },
        },
        "guess_raw_directory": guess_raw_directory_desy,
        "guess_experiment_id": guess_experiment_id_desy,
        "guess_batch_queue": guess_batch_queue_desy,
        "kill_processing_job": kill_slurm_job,
    },
}
"""
Supported facilities, instruments and detectors.

This dictionary contains information about supported facilities, instruments and
detectors and associated with them resources, functions and classes. The keys of the 
dictionary are the names of the supported facilities and the values are 
[TypeFacilityInfo][cheetah.crawlers.TypeFacilityInfo] dictionaries.
"""

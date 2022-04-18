"""
Cheetah YAML dumper.
"""
import pathlib
import yaml

from typing import Any


class CheetahSafeDumper(yaml.SafeDumper):
    """
    Cheetah safe YAML dumper.

    This class subclasses yaml.SafeDumper adding representation of the pathlib.PosixPath
    objects as strings.
    """

    def represent_data(self, data: Any) -> Any:
        if isinstance(data, pathlib.PosixPath):
            return self.represent_scalar("tag:yaml.org,2002:str", str(data))
        return super().represent_data(data)

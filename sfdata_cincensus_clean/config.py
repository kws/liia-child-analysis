from copy import deepcopy
from functools import cached_property
from pathlib import Path
from typing import Mapping, Iterator, Any

import yaml

import wrangling


def _get_keys(obj, mapping, prefix: list = None):
    if not hasattr(obj, "keys"):
        return mapping

    keys = [k for k in obj.keys() if k[0].isupper()]
    for key in keys:
        if prefix is None:
            mapping_key = key
        else:
            mapping_key = "/".join(prefix + [key])
        mapping[mapping_key] = obj[key]
        del obj[key]
        _get_keys(
            mapping[mapping_key], mapping, prefix=prefix + [key] if prefix else None
        )
    return mapping


class Config(Mapping[str, Any]):
    def __init__(self, filename=None):
        if filename is None:
            self._path = Path(wrangling.__file__).parent / "config/cin_datamap.yaml"
        else:
            self._path = Path(filename)

        with open(self._path) as FILE:
            self._config = yaml.safe_load(FILE)

    @cached_property
    def fields(self) -> Mapping[str, Any]:
        return _get_keys(deepcopy(self._config), {})

    def fields_with_prefix(self, prefix: list = None) -> Mapping[str, Any]:
        return _get_keys(deepcopy(self._config), {}, prefix)

    def __getitem__(self, key):
        return self._config[key]

    def __iter__(self) -> Iterator[Any]:
        return self._config.__iter__()

    def __len__(self) -> int:
        return self._config.__len__()

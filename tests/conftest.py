import pytest
import yaml

import wrangling
from pathlib import Path


@pytest.fixture
def fixtures_path():
    return Path(__file__).parent.joinpath('fixtures')


@pytest.fixture
def config():
    config_file = Path(wrangling.__file__).parent / 'config/cin_datamap.yaml'
    with open(config_file) as FILE:
        return yaml.safe_load(FILE)

import pytest
from pathlib import Path


@pytest.fixture
def fixtures_path():
    return Path(__file__).parent.joinpath('fixtures')


import pytest
from kp_demo.cli import fixture_records


@pytest.fixture
def rows():
    return fixture_records()

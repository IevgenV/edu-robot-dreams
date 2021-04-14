import os
import pathlib
import shutil
from datetime import date

import pytest

from .Cache import FileDailyCache
from .Server import ProdServer

default_config_path = pathlib.Path("../configs/main.yml")
temp_cache_dir = pathlib.Path("./data")


@pytest.fixture()
def change_test_dir_and_create_data_path(request):
    """ The fixture required to change the root directory where from pytest runs the tests.
        Root is set to the directory whene this file is located, so relative paths work fine. """
    os.chdir(request.fspath.dirname)  # <- Changes pytest root to this directory
    temp_cache_dir.mkdir(exist_ok=True)  # <- `temp_cache_dir` shouldn't exist before this line run
    yield
    if temp_cache_dir.exists():
        shutil.rmtree(temp_cache_dir, ignore_errors=True)
    os.chdir(request.config.invocation_dir)  # <- Revert pytest root

@pytest.fixture
def server_datasource(change_test_dir_and_create_data_path):
    """ Creates default ProdServer based on the default configuration and default server name. """
    return ProdServer(default_config_path)

@pytest.fixture
def def_cache(server_datasource):
    """ Creates default FileDailyCache based on the default dProdServer DataSource. """
    dailyCache = FileDailyCache(temp_cache_dir, server_datasource)
    yield dailyCache
    dailyCache.clear()

def test_FileDailyCache_general(def_cache):
    """ Test of FileDailyCache main functionalities: `update()` and `get_data()`. """
    def_cache.update()
    cached_data = def_cache.get_data()
    assert len(cached_data) == 1

def test_FileDailyCache_set_date(def_cache):
    """ Test of FileDailyCache main functionalities: `update()` and `get_data()`. """
    def_cache.set_cache_date(date(2021, 1, 3))
    def_cache.update()
    cached_data = def_cache.get_data()
    assert len(cached_data) == 1

import os
import pathlib
from datetime import date

import pytest
import requests

from .Server import ProdServer


default_config_path = pathlib.Path("../configs/main.yml")
expired_token = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJleHAiOjE2MTc4ODMwODMsImlhdCI6MTYxNzg4Mjc4MywibmJmIjoxNjE3ODgyNzgzLCJpZGVudGl0eSi6MX0.-6m9MzmbBN1H9yGdtH799YZMiKuumgx-rwit_HllxyQ"
wrong_address = "https://robot-dreams-de-api.not.a.herokuapp.com"


@pytest.fixture()
def change_test_dir(request):
    """ The fixture required to change the root directory where from pytest runs the tests.
        Root is set to the directory whene this file is located, so relative paths work fine. """
    os.chdir(request.fspath.dirname)  # <- Changes pytest root to this directory
    yield
    os.chdir(request.config.invocation_dir)  # <- Revert pytest root

@pytest.fixture
def def_server(change_test_dir):
    """ Creates default ProdServer based on the default configuration and default server name. """
    return ProdServer(default_config_path)

def test_ProdServer_incorrect_configuration(def_server):
    """ Test of ProdServer construction with incorrect configuration file path. """
    with pytest.raises(FileNotFoundError):
        ProdServer(pathlib.Path("dummy/config/path.yml"))

def test_get_data_by_date_correct(def_server):
    """ Test of ProdServer::get_data_by_date() method correct call. """
    products = def_server.get_data_by_date(
        date(2021, 1, 2),
        date(2021, 1, 3))
    assert isinstance(products, dict)
    assert len(products) == 2

def test_get_data_by_date_wrong_date_order(def_server):
    """ Test of ProdServer::get_data_by_date() method with incorrectly ordered dates. """
    with pytest.raises(AssertionError):
        products = def_server.get_data_by_date(
            date(2021, 1, 3),
            date(2021, 1, 2))

def test_get_data_by_date_correct(def_server):
    """ Test of ProdServer::get_data_by_date() method correct call. """
    today_date = date.today()
    products1 = def_server.get_data_by_date(today_date)
    products2 = def_server.get_data_by_date(today_date, today_date)
    assert len(products1) == len(products2) == 1
    # NOTE(IevgenV): Following condition can't be guaranteed if list of today's out of stock
    # products is updated dynamically, but let's check it in educational purposes:
    assert len(products1[today_date]) == len(products2[today_date])

def test_get_data_by_date_emulate_expired_token(def_server):
    today_date = date.today()
    prods = def_server.get_data_by_date(today_date)
    assert len(prods) == 1
    def_server.token = expired_token  # <- Faking token with expired/bad one
    def_server.get_data_by_date(today_date)  # <- Request of new valid token is expected
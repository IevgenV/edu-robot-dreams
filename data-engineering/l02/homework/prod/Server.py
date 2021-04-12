import json
from datetime import date, timedelta

import pytest
import requests
import yaml


class ProdServer:
    
    def __init__(self, cfg_filepath:str, server_name:str="prod_server"):
        assert isinstance(cfg_filepath, str), \
                "Configuration file must be specified with a Python string."

        with open(cfg_filepath) as cfg_fp:
            cfg = yaml.safe_load(cfg_fp)

        self.login = cfg[server_name]["login"]
        self.passwd = cfg[server_name]["password"]

        self.address = cfg[server_name]["address"]
        self.authorize_api = cfg[server_name]["apis"]["authorize"]
        self.out_of_stock_api = cfg[server_name]["apis"]["out_of_stock"]

        self.token = self.__generate_token()

    def get_out_of_stock_by_date(self, date_start:date, date_end:date):
        assert date_start <= date_end, "start_date has to be less or equal to end_date"

        url = self.address + self.out_of_stock_api["endpoint"]
        headers = {"content-type": "application/json", "Authorization": self.token}

        products = {}
        request_date = date_start
        while request_date <= date_end:
            data = {"date": request_date.isoformat()}  # ISO: "YYYY-MM-DD"
            r = requests.get(url, data=json.dumps(data), headers=headers)
            r.raise_for_status()
            products[request_date] = r.json()
            request_date += timedelta(1)

        return products

    def get_out_of_stock_today(self):
        today_date = date.today()
        return self.get_out_of_stock_by_date(today_date, today_date)

    def __generate_token(self):
        url = self.address + self.authorize_api["endpoint"]
        headers = {"content-type": "application/json"}
        data = {"username": self.login, "password": self.passwd}
        r = requests.post(url, data=json.dumps(data), headers=headers)
        r.raise_for_status()
        r_json = r.json()
        self.token = "JWT " + r_json["access_token"]
        return self.token
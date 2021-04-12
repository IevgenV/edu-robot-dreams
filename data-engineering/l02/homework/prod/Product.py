import yaml

import ProdServer


class Product:
    
    def __init__(self, server : ProdServer):
        if server is None:
            server = ProdServer(cfg_filepath)
        assert isinstance(server, ProdServer)

        self.server = server

    def show_login(self):
        print(self.login, " : ", self.passwd)
        

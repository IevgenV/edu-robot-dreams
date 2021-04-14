from argparse import ArgumentParser
import pathlib
from datetime import datetime
from datetime import date

from prod.Cache import FileDailyCache
from prod.Server import ProdServer


DEF_SERVER_NAME = "prod_server"
DEF_CFG_FILE    = "./configs/main.yml"
DEF_CACHE_FILE  = "./cache"
DEF_CACHE_DATE  = date.today()


def prepare_argument_parser() -> ArgumentParser:

    parser = ArgumentParser(
            description="Caches out of stock products from remote server and returns the path to " \
                        "the file with cached data.")
    
    parser.add_argument("--config", dest="cfg_path", type=str,
        default=DEF_CFG_FILE,
        help="Path to the file with configuration in YAML format.")

    parser.add_argument("--name", dest="srv_name", type=str,
        default=DEF_SERVER_NAME,
        help="Name of the data server. " \
             "Will be used to parse correct section from configuration " \
             "YAML file (see --config option).")

    parser.add_argument("--cache", dest="cch_path", type=str,
        default=DEF_CACHE_FILE,
        help="Path to the directory where the cache will be stored.")

    parser.add_argument("--date", dest="cch_date", type=str,
        default=DEF_CACHE_DATE.isoformat(),
        help="Date in 'YYYY-MM-DD' format for which the data will be cached")

    return parser


if __name__ == "__main__":
    # Parse command line arguments:
    parser = prepare_argument_parser()
    args = parser.parse_args()
    configuration_file = pathlib.Path(args.cfg_path)
    cache_directory = pathlib.Path(args.cch_path)
    server_name = args.srv_name
    cache_date = datetime.strptime(args.cch_date, "%Y-%m-%d").date()

    # Create ProdServer object to request remote HTTP server for out of stock products:
    server = ProdServer(configuration_file, server_name)
    # Create FileDailyCache object to store (cache) retrieved product data into directory: 
    cache = FileDailyCache(cache_directory, server, cache_date)

    data = cache.get_data()  # <- Gets data and cache it at disk if not cached yet
    print(cache.get_cache_filepath())

import abc
import json
import pathlib
import shutil
from datetime import date
from typing import Optional

from .Server import DailyDataSource


class Cache(metaclass=abc.ABCMeta):
    """ Interface for any cache object providing `get_data()` and `update()` functionalities only. """

    @abc.abstractmethod
    def get_data(self) -> Optional[dict]:
        """ Returns the data.
        
        If data is already cached, it will be retuened from cache. In other case, the data will be
        requested from data source first. Subsequent calls will return cached values. For updating
        the cache, 'update()' method should be used.

            Optional[dict]: Data dictionary. Keys of the dictionary are used to determine whether
                            the data was cached or not. I.e., existed in dictionary keys identify
                            cached data. The more data is cached, the more keys are available. If
                            None is returned, then data is not availbale nor from cache, nor from
                            data source.
        """
        pass

    @abc.abstractmethod
    def update(self) -> bool:
        """ Updates the data in cache.

        Update of the data will be forced even if data is already cached. `update()` will forcibly
        request the data source for the data ignoring the current cache.

        Returns:
            bool: True if the data source returned data to be cached. False otherwise. The latter
                  means that cache isn't updated.
        """
        pass

    def clear(self) -> bool:
        """ Clears all the cache. It will delete all the cahce. So, use carefully.

        Returns:
            bool: True if the cache was existed before the clear. False otherwise.
        """
        pass


class FileDailyCache(Cache):

    def __init__(self, cache_dir:pathlib.Path, data_source:DailyDataSource, cache_date:date=None):
        assert isinstance(cache_dir, pathlib.Path)
        assert isinstance(data_source, DailyDataSource)
        cache_date = date.today() if cache_date is None else cache_date
        assert isinstance(cache_date, date)

        self.cache_dir = cache_dir
        self.data_source = data_source
        self.cache_date = cache_date

    def get_data(self) -> Optional[dict]:
        day = self.cache_date
        cache_filepath = self.cache_dir / pathlib.Path(f"{day}/{day}.json")
        if not cache_filepath.exists() and not self.update():
            return None  # <- Data for cache day is not available
        with open(cache_filepath, 'r') as cache_file:
            return {day: json.load(cache_file)}

    def update(self) -> bool:
        new_data = self.data_source.get_data_by_date(self.cache_date)
        if self.cache_date not in new_data:
            return False
        self.save_data_to_file(new_data[self.cache_date])
        return True

    def set_cache_date(self, cache_date:date):
        assert isinstance(cache_date, date)
        self.cache_date = cache_date

    def save_data_to_file(self, data:dict) -> None:
        filepath = self.get_cache_filepath()
        cache_subdir = filepath.parents[0]  # <- Get directory where file is located
        cache_subdir.mkdir(parents=True, exist_ok=True)
        with open(filepath, 'w') as cache_file:
            json.dump(data, cache_file)

    def clear(self) -> bool:
        if self.cache_dir.exists():
            shutil.rmtree(self.cache_dir, ignore_errors=True)

    def get_cache_filepath(self) -> pathlib.Path:
        day_str = self.cache_date.isoformat()  # ISO: "YYYY-MM-DD"
        cache_subdir = self.cache_dir / pathlib.Path(day_str)
        cache_filepath = cache_subdir / pathlib.Path(f"{day_str}.json")
        return cache_filepath
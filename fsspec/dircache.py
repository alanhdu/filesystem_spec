import time
from typing import MutableMapping, List, Dict, Any, Optional, Iterator, TypedDict
from functools import lru_cache


class Entry(TypedDict):
    name: str
    size: int
    type: str



class DirCache(MutableMapping[str, List[Entry]]):
    """
    Caching of directory listings, in a structure like::

        {"path0": [
            {"name": "path0/file0",
             "size": 123,
             "type": "file",
             ...
            },
            {"name": "path0/file1",
            },
            ...
            ],
         "path1": [...]
        }

    Parameters to this class control listing expiry or indeed turn
    caching off
    """

    def __init__(
        self,
        use_listings_cache: bool=True,
        listings_expiry_time: Optional[float]=None,
        max_paths: Optional[int]=None,
        **kwargs: Any,
    ) -> None:
        """

        Parameters
        ----------
        use_listings_cache: bool
            If False, this cache never returns items, but always reports KeyError,
            and setting items has no effect
        listings_expiry_time: int or float (optional)
            Time in seconds that a listing is considered valid. If None,
            listings do not expire.
        max_paths: int (optional)
            The number of most recent listings that are considered valid; 'recent'
            refers to when the entry was set.
        """
        self._cache: Dict[str, List[Entry]] = {}
        self._times: Dict[str, float] = {}
        if max_paths:
            self._q = lru_cache(max_paths + 1)(lambda key: self._cache.pop(key, None))
        self.use_listings_cache = use_listings_cache
        self.listings_expiry_time = listings_expiry_time
        self.max_paths = max_paths

    def __getitem__(self, item: str) -> List[Entry]:
        if self.listings_expiry_time is not None:
            if self._times.get(item, 0) - time.time() < -self.listings_expiry_time:
                del self._cache[item]
        if self.max_paths:
            self._q(item)
        return self._cache[item]  # maybe raises KeyError

    def clear(self) -> None:
        self._cache.clear()

    def __len__(self) -> int:
        return len(self._cache)

    def __contains__(self, item: object) -> bool:
        try:
            self[item]  # type: ignore[index]
            return True
        except KeyError:
            return False

    def __setitem__(self, key: str, value: List[Entry]) -> None:
        if not self.use_listings_cache:
            return
        if self.max_paths:
            self._q(key)
        self._cache[key] = value
        if self.listings_expiry_time is not None:
            self._times[key] = time.time()

    def __delitem__(self, key: str) -> None:
        del self._cache[key]

    def __iter__(self) -> Iterator[str]:
        entries = list(self._cache)

        return (k for k in entries if k in self)

    def __reduce__(self):
        return (
            DirCache,
            (self.use_listings_cache, self.listings_expiry_time, self.max_paths),
        )

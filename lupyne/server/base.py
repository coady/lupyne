import time
import lucene
from .. import engine


class WebSearcher:
    """Dispatch root with a delegated Searcher."""

    def __init__(self, *directories, **kwargs):
        if len(directories) > 1:  # pragma: no cover
            self._searcher = engine.MultiSearcher(directories, **kwargs)
        else:
            self._searcher = engine.IndexSearcher(*directories, **kwargs)
        self.updated = time.time()

    def close(self):
        """Explicit close for clean shutdown."""
        del self._searcher  # pragma: no cover

    @property
    def searcher(self) -> engine.IndexSearcher:
        """attached IndexSearcher"""
        lucene.getVMEnv().attachCurrentThread()
        return self._searcher

    @property
    def etag(self) -> str:
        """ETag header"""
        return f'W/"{self.searcher.version}"'

    @property
    def age(self) -> float:
        """Age header"""
        return time.time() - self.updated

    def index(self) -> dict:
        """Return index information."""
        searcher = self.searcher
        if isinstance(searcher, engine.MultiSearcher):  # pragma: no cover
            return {reader.directory().toString(): reader.numDocs() for reader in searcher.indexReaders}
        return {searcher.directory.toString(): len(searcher)}

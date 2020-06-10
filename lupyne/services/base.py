import time
import lucene
from org.apache.lucene import index
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
        """index information"""
        searcher = self.searcher
        if isinstance(searcher, engine.MultiSearcher):  # pragma: no cover
            return {reader.directory().toString(): reader.numDocs() for reader in searcher.indexReaders}
        return {searcher.directory.toString(): len(searcher)}

    def refresh(self, spellcheckers: bool = False) -> dict:
        """Refresh index version."""
        self._searcher = self.searcher.reopen(spellcheckers=spellcheckers)
        self.updated = time.time()
        return self.index()

    def indexed(self) -> list:
        """indexed field names"""
        fieldinfos = self.searcher.fieldinfos.values()
        return sorted(fi.name for fi in fieldinfos if fi.indexOptions != index.IndexOptions.NONE)
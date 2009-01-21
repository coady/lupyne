import unittest, os
import tempfile, shutil
import itertools
import lucene
lucene.initVM(lucene.CLASSPATH)
import engine
import fixture

class LocalTest(unittest.TestCase):
    def setUp(self):
        self.tempdir = tempfile.mkdtemp(dir=os.path.dirname(__file__))
    def tearDown(self):
        shutil.rmtree(self.tempdir)
    
    def testInterface(self):
        self.assertRaises(lucene.JavaError, engine.Indexer, self.tempdir, 'r')
        index = engine.Indexer(self.tempdir)
        self.assertRaises(lucene.JavaError, engine.IndexWriter, self.tempdir)
        reader = engine.IndexReader(self.tempdir)
        searcher = engine.IndexSearcher(self.tempdir)
        assert len(reader) == len(searcher) == 0
        assert list(reader) == list(searcher) == []
        reader.close()
        del reader, searcher
        index.set('text')
        index.set('name', store=True, index=False)
        index.set('rank', index=True)
        for n in range(26):
            index.add(text='hello world', name=chr(ord('a')+n), rank='%02i' % n)
        assert len(index) == 26
        assert list(index) == []
        index.commit()
        assert list(index) == range(26)
        assert index.current and index.optimized
        assert 0 in index and 26 not in index
        doc = index[0]
        assert dict(doc.items()) == {'name': 'a'}
        assert list(doc) == ['name'] and doc['name'] == 'a'
        self.assertRaises(KeyError, doc.__getitem__, 'key')
        assert doc.getlist('name') == ['a'] and doc.getlist('key') == []
        assert index.count('text', 'hello') == index.count('text:hello') == 26
        assert sorted(index.names()) == ['name', 'rank', 'text']
        assert sorted(index.names('indexed')) == ['rank', 'text']
        assert index.names('unindexed') == ['name']
        assert list(index.terms('text')) == ['hello', 'world']
        assert dict(index.terms('text', 'w', counts=True)) == {'world': 26}
        assert list(index.terms('test')) == []
        assert list(index.docs('text', 'hello')) == range(26)
        assert list(index.docs('text', 'hi')) == []
        assert all(count == 1 for doc, count in index.docs('text', 'hello', counts=True))
        assert all(positions == [0] for doc, positions in index.positions('text', 'hello'))
        hits = index.search('text:hello')
        assert len(hits) == hits.count == 26
        score, = set(hits.scores)
        assert dict(hits.items()) == dict.fromkeys(range(26), score)
        hits = index.search('text:hello', count=10)
        assert len(hits) == 10 and hits.count == 26
        for hit in hits:
            assert 0 <= hit.id < 26 and hit.score == score
            data = dict(hit.items())
            assert data['__id__'] == hit.id and 'name' in data
        assert not index.search('hello') and index.search('hello', field='text')
        assert index.search('text:hello hi') and not index.search('text:hello hi', op='and')
        hits = index.search('text:hello', count=3, sort=['rank'])
        assert hits.ids == range(3)
        hits = index.search('text:hello', count=3, sort='rank', reverse=True)
        assert hits.ids == range(25, 22, -1)
        hits = index.search('text:hello', filter=lucene.PrefixFilter(lucene.Term('rank', '2')))
        assert hits.ids == range(20, 26)
        assert hits[2:4].ids == range(22, 24)
        index.delete('rank:00')
        assert 0 in index
        assert len(index) == sum(index.segments.values())
        index.commit()
        assert len(index) < sum(index.segments.values())
        index.optimize()
        assert len(index) == sum(index.segments.values())
        assert 0 not in index
        temp = engine.IndexWriter()
        temp.add()
        temp.commit()
        index += temp.directory
        assert len(index) == 26
        del index
        assert engine.Indexer(self.tempdir)
    
    def testData(self):
        index = engine.Indexer(self.tempdir)
        for name, params in fixture.fields():
            index.set(name, **params)
        for doc in fixture.docs():
            index.add(doc)
        assert len(index) == 35 and index.names() == []
        index.commit()
        assert sorted(index.names()) == ['amendment', 'article', 'date', 'text']
        articles = list(index.terms('article'))
        articles.remove('Preamble')
        assert sorted(map(int, articles)) == range(1, 8)
        assert sorted(map(int, index.terms('amendment'))) == range(1, 28)
        assert list(itertools.islice(index.terms('text', 'right'), 2)) == ['right', 'rights']
        word, count = next(index.terms('text', 'people', counts=True))
        assert word == 'people' and count == 8
        docs = dict(index.docs('text', 'people', counts=True))
        counts = docs.values()
        assert len(docs) == count and all(counts) and sum(counts) > count
        positions = dict(index.positions('text', 'people'))
        assert map(len, positions.values()) == counts
        hit, = index.search('"We the People"', field='text')
        assert hit['article'] == 'Preamble'
        assert sorted(dict(hit.items())) == ['__id__', '__score__', 'article']
        hits = index.search('people', field='text')
        assert len(hits) == hits.count == 8
        ids = hits.ids
        hits = index.search('people', count=5, field='text')
        assert hits.ids == ids[:len(hits)]
        assert len(hits) == 5 and hits.count == 8
        hits = index.search('text:people', count=5, sort='amendment')
        assert sorted(hits.ids) == hits.ids and hits.ids != ids[:len(hits)]
        hit, = index.search('freedom', field='text')
        assert hit['amendment'] == '1'
        assert sorted(dict(hit.items())) == ['__id__', '__score__', 'amendment', 'date']
        hits = index.search('text:right')
        hits = index.search('text:people', filter=hits.ids)
        assert len(hits) == 4

if __name__ == '__main__':
    unittest.main()

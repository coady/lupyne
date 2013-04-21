"""
Basic indexing and searching example adapted from http://lucene.apache.org/java/3_6_2/api/core/index.html
"""

import lucene
lucene.initVM()
try:
    from org.apache.lucene import document, index, search, store, util
    from org.apache.lucene.queryparser import classic as queryParser
    from org.apache.lucene.analysis import standard
except ImportError:
    document = index = queryParser = search = store = util = standard = lucene
from lupyne import engine

### lucene ###

analyzer = standard.StandardAnalyzer(util.Version.LUCENE_CURRENT)

# Store the index in memory:
directory = store.RAMDirectory()
# To store an index on disk, use this instead:
#Directory directory = FSDirectory.open(File("/tmp/testindex"))
config = index.IndexWriterConfig(util.Version.LUCENE_CURRENT, analyzer)
iwriter = index.IndexWriter(directory, config)
doc = document.Document()
text = "This is the text to be indexed."
doc.add(document.Field("fieldname", text, document.Field.Store.YES, document.Field.Index.ANALYZED))
iwriter.addDocument(doc)
iwriter.close()

# Now search the index:
ireader = index.IndexReader.open(directory) # read-only=true
isearcher = search.IndexSearcher(ireader)
# Parse a simple query that searches for "text":
parser = queryParser.QueryParser(util.Version.LUCENE_CURRENT, "fieldname", analyzer)
query = parser.parse("text")
hits = isearcher.search(query, None, 1000).scoreDocs
assert len(hits) == 1
# Iterate through the results:
for hit in hits:
    hitDoc = isearcher.doc(hit.doc)
    assert hitDoc['fieldname'] == text
ireader.close()
directory.close()

### lupyne ###

# Store the index in memory:
indexer = engine.Indexer()              # Indexer combines Writer and Searcher; RAMDirectory and StandardAnalyzer are defaults
indexer.set('fieldname', store=True)    # settings for all documents of indexer; analyzed is the default
indexer.add(fieldname=text)             # add document
indexer.commit()                        # commit changes and refresh searcher

# Now search the index:
hits = indexer.search('text', field='fieldname')    # parsing handled if necessary
assert len(hits) == 1
for hit in hits:                                    # hits support mapping interface
    assert hit['fieldname'] == text
# closing is handled automatically

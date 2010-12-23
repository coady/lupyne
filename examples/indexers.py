"""
Basic indexing and searching example adapted from http://lucene.apache.org/java/2_9_1/api/core/index.html
Compatible with lucene versions 2.9 through 3.0.
"""

import lucene
lucene.initVM()
from lupyne import engine

### lucene ###

analyzer = lucene.StandardAnalyzer(lucene.Version.LUCENE_CURRENT)

# Store the index in memory:
directory = lucene.RAMDirectory()
# To store an index on disk, use this instead:
#Directory directory = FSDirectory.open(lucene.File("/tmp/testindex"))
iwriter = lucene.IndexWriter(directory, analyzer, True, lucene.IndexWriter.MaxFieldLength(25000))
doc = lucene.Document()
text = "This is the text to be indexed."
doc.add(lucene.Field("fieldname", text, lucene.Field.Store.YES, lucene.Field.Index.ANALYZED))
iwriter.addDocument(doc)
iwriter.close()

# Now search the index:
isearcher = lucene.IndexSearcher(directory)
# Parse a simple query that searches for "text":
parser = lucene.QueryParser(lucene.Version.LUCENE_CURRENT, "fieldname", analyzer)
query = parser.parse("text")
hits = isearcher.search(query, None, 1000).scoreDocs
assert len(hits) == 1
# Iterate through the results:
for hit in hits:
    hitDoc = isearcher.doc(hit.doc)
    assert hitDoc['fieldname'] == text
isearcher.close()
directory.close()

### lupyne ###

# Store the index in memory:
indexer = engine.Indexer()              # Indexer combines Writer and Searcher; RAMDirectory and StandardAnalyzer are defaults
indexer.set('fieldname', store=True)    # settings for all documents of indexer; tokenized is the default
indexer.add(fieldname=text)             # add document
indexer.commit()                        # commit changes and refresh searcher

# Now search the index:
hits = indexer.search('text', field='fieldname')    # parsing handled if necessary
assert len(hits) == 1
for hit in hits:                                    # hits support mapping interface
    assert hit['fieldname'] == text
# closing is handled automatically

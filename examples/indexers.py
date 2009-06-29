"""
Basic indexing and searching example adapted from http://lucene.apache.org/java/2_4_1/api/core/index.html
"""

import lucene
lucene.initVM(lucene.CLASSPATH)
from lupyne import engine

### lucene ###

analyzer = lucene.StandardAnalyzer()

# Store the index in memory:
directory = lucene.RAMDirectory()
# To store an index on disk, use this instead:
#Directory directory = FSDirectory.getDirectory("/tmp/testindex")
iwriter = lucene.IndexWriter(directory, analyzer, True)
iwriter.setMaxFieldLength(25000)
doc = lucene.Document()
text = "This is the text to be indexed."
doc.add(lucene.Field("fieldname", text, lucene.Field.Store.YES, lucene.Field.Index.TOKENIZED))
iwriter.addDocument(doc)
iwriter.optimize()
iwriter.close()

# Now search the index:
isearcher = lucene.IndexSearcher(directory)
# Parse a simple query that searches for "text":
parser = lucene.QueryParser("fieldname", analyzer)
query = parser.parse("text")
hits = isearcher.search(query)
assert 1 == hits.length()
# Iterate through the results:
for i in range(hits.length()):
    hitDoc = hits.doc(i)
    assert "This is the text to be indexed." == hitDoc.get("fieldname")
isearcher.close()
directory.close()

### lupyne ###

# Store the index in memory:
indexer = engine.Indexer()              # Indexer combines Writer and Searcher; RAMDirectory and StandardAnalyzer are defaults
indexer.maxFieldLength = 25000          # not necessary for this example
indexer.set('fieldname', store=True)    # settings for all documents of indexer; tokenized is the default
indexer.add(fieldname=text)             # add document
indexer.optimize()                      # not necessary for this example
indexer.commit()                        # commit changes and refresh searcher

# Now search the index:
hits = indexer.search('text', field='fieldname')    # parsing handled if necessary
assert len(hits) == 1                               # hits support full sequence interface
for hit in hits:
    assert hit['fieldname'] == text                 # hit supports full mapping interface
# closing is handled automatically

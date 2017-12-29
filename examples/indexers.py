"""
Basic indexing and searching example adapted from http://lucene.apache.org/core/7_2_0/core/index.html
"""

import lucene
from org.apache.lucene import analysis, document, index, queryparser, search, store
from lupyne import engine
assert lucene.getVMEnv() or lucene.initVM()

# # # lucene # # #

analyzer = analysis.standard.StandardAnalyzer()

# Store the index in memory:
directory = store.RAMDirectory()
# To store an index on disk, use this instead:
# Directory directory = FSDirectory.open(File("/tmp/testindex"))
config = index.IndexWriterConfig(analyzer)
iwriter = index.IndexWriter(directory, config)
doc = document.Document()
text = "This is the text to be indexed."
doc.add(document.Field('fieldname', text, document.TextField.TYPE_STORED))
iwriter.addDocument(doc)
iwriter.close()

# Now search the index:
ireader = index.DirectoryReader.open(directory)
isearcher = search.IndexSearcher(ireader)
# Parse a simple query that searches for "text":
parser = queryparser.classic.QueryParser('fieldname', analyzer)
query = parser.parse('text')
hits = isearcher.search(query, 1000).scoreDocs
assert len(hits) == 1
# Iterate through the results:
for hit in hits:
    hitDoc = isearcher.doc(hit.doc)
    assert hitDoc['fieldname'] == text
ireader.close()
directory.close()

# # # lupyne # # #

# Store the index in memory:
indexer = engine.Indexer()                                  # Indexer combines Writer and Searcher; RAMDirectory and StandardAnalyzer are defaults
indexer.set('fieldname', engine.Field.Text, stored=True)    # default indexed text settings for documents
indexer.add(fieldname=text)                                 # add document
indexer.commit()                                            # commit changes and refresh searcher

# Now search the index:
hits = indexer.search('text', field='fieldname')    # parsing handled if necessary
assert len(hits) == 1
for hit in hits:                                    # hits support mapping interface
    assert hit['fieldname'] == text
# closing is handled automatically

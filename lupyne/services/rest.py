import math
import time
from typing import Union
import lucene
from fastapi import FastAPI
from .settings import DEBUG, DIRECTORIES
from .base import Document, WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = FastAPI(debug=DEBUG)
app.on_event('shutdown')(root.close)

app.get('/', response_description="{`directory`: `count`}")(root.index)
app.post('/', response_description="{`directory`: `count`}")(root.refresh)
app.get('/terms', response_description="[`name`, ...]")(root.indexed)


@app.get('/terms/{name}')
def terms(name: str, *, counts: bool = False) -> Union[list, dict]:
    terms = root.searcher.terms(name, counts=counts)
    return (dict if counts else list)(terms)


@app.get('/search')
def search(q: str, count: int = None, sort: str = '') -> dict:
    """Run query and return hits."""
    sortfields = root.sortfields(sort and sort.split(','))
    hits = root.searcher.search(q, count, list(sortfields.values()) or None)
    result = {'count': hits.count, 'hits': []}
    for hit in hits:
        score = None if math.isnan(hit.score) else hit.score
        sortkeys = dict(zip(sortfields, hit.sortkeys))
        doc = Document(**hit).__dict__
        result['hits'].append({'id': hit.id, 'score': score, 'sortkeys': sortkeys, 'doc': doc})
    return result


@app.middleware('http')
async def headers(request, call_next):
    start = time.time()
    response = await call_next(request)
    response.headers.update({'x-response-time': str(time.time() - start), 'age': str(int(root.age)), 'etag': root.etag})
    return response

import time
from typing import Union
import lucene
from fastapi import FastAPI
from .settings import DEBUG, DIRECTORIES
from .base import WebSearcher

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


@app.middleware('http')
async def headers(request, call_next):
    start = time.time()
    response = await call_next(request)
    response.headers.update({'x-response-time': str(time.time() - start), 'age': str(int(root.age)), 'etag': root.etag})
    return response

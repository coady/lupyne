import time
import lucene
from fastapi import FastAPI
from .settings import DEBUG, DIRECTORIES
from .base import WebSearcher

assert lucene.getVMEnv() or lucene.initVM()
root = WebSearcher(*DIRECTORIES)
app = FastAPI(debug=DEBUG)
app.on_event('shutdown')(root.close)
app.get('/', response_description="{`directory`: `count`}")(root.index)


@app.middleware('http')
async def headers(request, call_next):
    start = time.time()
    response = await call_next(request)
    response.headers.update({'x-response-time': str(time.time() - start), 'age': str(int(root.age)), 'etag': root.etag})
    return response

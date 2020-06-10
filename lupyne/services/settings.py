from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings

config = Config('.env')
DEBUG = config('DEBUG', cast=bool, default=False)
DIRECTORIES = config('DIRECTORIES', cast=CommaSeparatedStrings)
SCHEMA = config('SCHEMA', default='')

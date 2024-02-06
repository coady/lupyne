from pathlib import Path
from starlette.config import Config
from starlette.datastructures import CommaSeparatedStrings

config = Config('.env' if Path('.env').is_file() else None)
DEBUG = config('DEBUG', cast=bool, default=False)
DIRECTORIES = config('DIRECTORIES', cast=CommaSeparatedStrings)
SCHEMA = config('SCHEMA', default='')

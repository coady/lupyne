import argparse
import json
import os
import lucene
from .legacy import WebSearcher, WebIndexer, start

parser = argparse.ArgumentParser(description='Restful json cherrypy server.', prog='lupyne.server')
parser.add_argument('directories', nargs='*', metavar='directory', help='index directories')
parser.add_argument('-r', '--read-only', action='store_true', help='expose only read methods; no write lock')
parser.add_argument('-c', '--config', help='optional configuration file or json object of global params')
parser.add_argument(
    '--autoreload',
    type=float,
    metavar='SECONDS',
    help='automatically reload modules; replacement for engine.autoreload',
)
parser.add_argument(
    '--autoupdate', type=float, metavar='SECONDS', help='automatically update index version and commit any changes'
)
parser.add_argument('--real-time', action='store_true', help='search in real-time without committing')

args = parser.parse_args()
read_only = args.read_only or len(args.directories) > 1
kwargs = {'nrt': True} if args.real_time else {}
if read_only and (args.real_time or not args.directories):
    parser.error('incompatible read/write options')
if args.config and not os.path.exists(args.config):
    args.config = {'global': json.loads(args.config)}
assert lucene.initVM(vmargs='-Xrs,-Djava.awt.headless=true')
cls = WebSearcher if read_only else WebIndexer
root = cls(*args.directories, **kwargs)
start(root, config=args.config, autoreload=args.autoreload, autoupdate=args.autoupdate)

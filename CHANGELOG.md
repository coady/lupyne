# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## Unreleased
### Changed
* Python >=3.9 required
* `DateTimeField.within` defaults to local time

### Fixed
* Deprecation warnings
* Starlette >=0.36 compatible

### Removed
* Spatial field

## [3.1](https://pypi.org/project/lupyne/3.1/) - 2023-11-22
### Changed
* Python >=3.8 required
* PyLucene >=9.6 required

### Added
* Shape fields
* GraphQL search enhancements

## [3.0](https://pypi.org/project/lupyne/3.0/) - 2023-01-16
### Changed
* PyLucene >=9.1 required

### Removed
* [CherryPy](https://cherrypy.dev) server removed

## [2.5](https://pypi.org/project/lupyne/2.5/) - 2020-11-24
* Python >=3.7 required
* PyLucene 8.6 supported
* [CherryPy](https://cherrypy.dev) server deprecated

## [2.4](https://pypi.org/project/lupyne/2.4/) - 2019-12-14
* PyLucene >=8 required
* `Hit.keys` renamed to `Hit.sortkeys`

## [2.3](https://pypi.org/project/lupyne/2.3/) - 2019-09-11
* PyLucene >=7.7 required
* PyLucene 8 supported

## [2.2](https://pypi.org/project/lupyne/2.2/) - 2019-01-14
* PyLucene 7.6 supported

## [2.1](https://pypi.org/project/lupyne/2.1/) - 2018-10-19
* PyLucene >=7 required

## [2.0](https://pypi.org/project/lupyne/2.0/) - 2017-12-29
* PyLucene >=6 required
* Python 3 support
* client moved to external package

## [1.9](https://pypi.org/project/lupyne/1.9/) - 2015-09-09
* Python 2.6 dropped
* PyLucene 4.8 and 4.9 dropped
* IndexWriter implements context manager
* Server DocValues updated via patch method
* Spatial tile search optimized

## [1.8](https://pypi.org/project/lupyne/1.8/) - 2014-10-07
* PyLucene 4.10 supported
* PyLucene 4.6 and 4.7 dropped
* Comparator iteration optimized
* Support for string based FieldCacheRangeFilters

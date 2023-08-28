# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 0.8.0 (August 28th, 2023)

### Added

* Allow SQLite query parameters and support cached databases ([#561][#561])
* Support for unix socket for aiomysql and asyncmy ([#551][#551])

[#551]: https://github.com/encode/databases/pull/551
[#561]: https://github.com/encode/databases/pull/546

### Changed

* Change isolation connections and transactions during concurrent usage ([#546][#546])
* Bump requests from 2.28.1 to 2.31.0 ([#562][#562])
* Bump starlette from 0.20.4 to 0.27.0 ([#560][#560])
* Bump up asyncmy version to fix `No module named 'asyncmy.connection'` ([#553][#553])
* Bump wheel from 0.37.1 to 0.38.1 ([#524][#524])

[#546]: https://github.com/encode/databases/pull/546
[#562]: https://github.com/encode/databases/pull/562
[#560]: https://github.com/encode/databases/pull/560
[#553]: https://github.com/encode/databases/pull/553
[#524]: https://github.com/encode/databases/pull/524

### Fixed

* Fix the type-hints using more standard mode ([#526][#526])

[#526]: https://github.com/encode/databases/pull/526

## 0.7.0 (Dec 18th, 2022)

### Fixed

* Fixed breaking changes in SQLAlchemy cursor; supports `>=1.4.42,<1.5` ([#513][#513])
* Wrapped types in `typing.Optional` where applicable ([#510][#510])

[#513]: https://github.com/encode/databases/pull/513
[#510]: https://github.com/encode/databases/pull/510

## 0.6.2 (Nov 7th, 2022)

### Changed

* Pinned SQLAlchemy `<=1.4.41` to avoid breaking changes ([#520][#520])

[#520]: https://github.com/encode/databases/pull/520

## 0.6.1 (Aug 9th, 2022)

### Fixed

* Improve typing for `Transaction` ([#493][#493])
* Allow string indexing into Record ([#501][#501])

[#493]: https://github.com/encode/databases/pull/493
[#501]: https://github.com/encode/databases/pull/501

## 0.6.0 (May 29th, 2022)

* Dropped Python 3.6 support ([#458][#458])

[#458]: https://github.com/encode/databases/pull/458

### Added

* Add \_mapping property to the result set interface ([#447][#447])
* Add contributing docs ([#453][#453])

[#447]: https://github.com/encode/databases/pull/447
[#453]: https://github.com/encode/databases/pull/453

### Fixed

* Fix query result named access ([#448][#448])
* Fix connections getting into a bad state when a task is cancelled ([#457][#457])
* Revert #328 parallel transactions ([#472][#472])
* Change extra installations to specific drivers ([#436][#436])

[#448]: https://github.com/encode/databases/pull/448
[#457]: https://github.com/encode/databases/pull/457
[#472]: https://github.com/encode/databases/pull/472
[#436]: https://github.com/encode/databases/pull/436

## 0.5.4 (January 14th, 2022)

### Added

* Support for Unix domain in connections ([#423][#423])
* Added `asyncmy` MySQL driver ([#382][#382])

[#423]: https://github.com/encode/databases/pull/423
[#382]: https://github.com/encode/databases/pull/382

### Fixed

* Fix SQLite fetch queries with multiple parameters ([#435][#435])
* Changed `Record` type to `Sequence` ([#408][#408])

[#435]: https://github.com/encode/databases/pull/435
[#408]: https://github.com/encode/databases/pull/408

## 0.5.3 (October 10th, 2021)

### Added

* Support `dialect+driver` for default database drivers like `postgresql+asyncpg` ([#396][#396])

[#396]: https://github.com/encode/databases/pull/396

### Fixed

* Documentation of low-level transaction ([#390][#390])

[#390]: https://github.com/encode/databases/pull/390

## 0.5.2 (September 10th, 2021)

### Fixed

* Reset counter for failed connections ([#385][#385])
* Avoid dangling task-local connections after Database.disconnect() ([#211][#211])

[#385]: https://github.com/encode/databases/pull/385
[#211]: https://github.com/encode/databases/pull/211

## 0.5.1 (September 2nd, 2021)

### Added

* Make database `connect` and `disconnect` calls idempotent ([#379][#379])

[#379]: https://github.com/encode/databases/pull/379

### Fixed

* Fix `in_` and `notin_` queries in SQLAlchemy 1.4 ([#378][#378])

[#378]: https://github.com/encode/databases/pull/378

## 0.5.0 (August 26th, 2021)

### Added

* Support SQLAlchemy 1.4 ([#299][#299])

[#299]: https://github.com/encode/databases/pull/299

### Fixed

* Fix concurrent transactions ([#328][#328])

[#328]: https://github.com/encode/databases/pull/328

## 0.4.3 (March 26th, 2021)

### Fixed

* Pin SQLAlchemy to <1.4 ([#314][#314])

[#314]: https://github.com/encode/databases/pull/314

## 0.4.2 (March 14th, 2021)

### Fixed

* Fix memory leak with asyncpg for SQLAlchemy generic functions ([#273][#273])

[#273]: https://github.com/encode/databases/pull/273

## 0.4.1 (November 16th, 2020)

### Fixed

* Remove package dependency on the synchronous DB drivers ([#256][#256])

[#256]: https://github.com/encode/databases/pull/256

## 0.4.0 (October 20th, 2020)

### Added

* Use backend native fetch_val() implementation when available ([#132][#132])
* Replace psycopg2-binary with psycopg2 ([#204][#204])
* Speed up PostgresConnection fetch() and iterate() ([#193][#193])
* Access asyncpg Record field by key on raw query ([#207][#207])
* Allow setting min_size and max_size in postgres DSN ([#210][#210])
* Add option pool_recycle in postgres DSN ([#233][#233])
* Allow extra transaction options ([#242][#242])

[#132]: https://github.com/encode/databases/pull/132
[#204]: https://github.com/encode/databases/pull/204
[#193]: https://github.com/encode/databases/pull/193
[#207]: https://github.com/encode/databases/pull/207
[#210]: https://github.com/encode/databases/pull/210
[#233]: https://github.com/encode/databases/pull/233
[#242]: https://github.com/encode/databases/pull/242

### Fixed

* Fix type hinting for sqlite backend ([#227][#227])
* Fix SQLAlchemy DDL statements ([#226][#226])
* Make fetch_val call fetch_one for type conversion ([#246][#246])
* Unquote username and password in DatabaseURL ([#248][#248])

[#227]: https://github.com/encode/databases/pull/227
[#226]: https://github.com/encode/databases/pull/226
[#246]: https://github.com/encode/databases/pull/246
[#248]: https://github.com/encode/databases/pull/248

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## 0.5.3 (October 10th, 2021)

### Added

* Support `dialect+driver` for default database drivers like `postgresql+asyncpg` (#396)

### Fixed

* Documentation of low-level transaction (#390)

## 0.5.2 (September 10th, 2021)

### Fixed

* Reset counter for failed connections (#385)
* Avoid dangling task-local connections after Database.disconnect() (#211)

## 0.5.1 (September 2nd, 2021)

### Added

* Make database `connect` and `disconnect` calls idempotent (#379)

### Fixed

* Fix `in_` and `notin_` queries in SQLAlchemy 1.4 (#378)

## 0.5.0 (August 26th, 2021)

### Added
* Support SQLAlchemy 1.4 (#299)

### Fixed

* Fix concurrent transactions (#328)

## 0.4.3 (March 26th, 2021)

### Fixed

* Pin SQLAlchemy to <1.4 (#314)

## 0.4.2 (March 14th, 2021)

### Fixed

* Fix memory leak with asyncpg for SQLAlchemy generic functions (#273)

## 0.4.1 (November 16th, 2020)

### Fixed

* Remove package dependency on the synchronous DB drivers (#256)

## 0.4.0 (October 20th, 2020)

### Added

* Use backend native fetch_val() implementation when available (#132)
* Replace psycopg2-binary with psycopg2 (#204)
* Speed up PostgresConnection fetch() and iterate() (#193)
* Access asyncpg Record field by key on raw query (#207)
* Allow setting min_size and max_size in postgres DSN (#210)
* Add option pool_recycle in postgres DSN (#233)
* Allow extra transaction options (#242)

### Fixed

* Fix type hinting for sqlite backend (#227)
* Fix SQLAlchemy DDL statements (#226)
* Make fetch_val call fetch_one for type conversion (#246)
* Unquote username and password in DatabaseURL (#248)

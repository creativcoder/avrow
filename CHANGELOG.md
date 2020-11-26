# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## 0.2.1 - 2020-11-27

### Fixed
- Named schema resolution outside of union variants.
- Implicitly defined named schemas should resolve in union variants [#6](https://github.com/creativcoder/avrow/issues/6)
- Default values in union schema fields in records should parse correctly [#1](https://github.com/creativcoder/avrow/issues/1)

### Updated
- Documentation.

## 0.2.0 - 2020-10-10

### Changed

- Reader takes a reference to the schema.

## 0.1.0 - 2020-10-08

### Added

Initial implementation of
- avrow
- avrow-cli (av)

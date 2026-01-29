# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-01-29

### Added
- Initial release of PostgrestParser
- Support for all PostgREST filter operators (eq, neq, gt, gte, lt, lte, like, ilike, match, imatch, in, is, fts, plfts, phfts, wfts, cs, cd, ov, sl, sr, nxl, nxr, adj)
- Filter operator negation (not.eq, not.in, not.like, etc.)
- Logic tree support (and, or, nested logic)
- Column selection with aliases and wildcards
- Resource embedding with LATERAL JOINs (M2O, O2M, O2O, M2M)
- Ordering with direction and nulls handling
- Pagination with limit and offset
- JSON path operations (arrow operators, array indexing)
- Full-text search operators
- Range and array operators
- Schema cache with GenServer
- Parameterized SQL query generation for SQL injection prevention
- Comprehensive test suite with unit and integration tests

### Security
- All SQL generation uses parameterized queries
- Proper identifier quoting and escaping
- Input validation at parser boundaries

[unreleased]: https://github.com/supabase/postgrest_parser/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/supabase/postgrest_parser/releases/tag/v0.1.0

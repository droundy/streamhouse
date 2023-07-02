# Streamhouse &emsp; [![Latest version](https://img.shields.io/crates/v/streamhouse.svg)](https://crates.io/crates/streamhouse) [![Documentation](https://docs.rs/streamhouse/badge.svg)](https://docs.rs/streamhouse) ![Build status](https://github.com/droundy/streamhouse/actions/workflows/rust.yml/badge.svg)

A strongly typed client for ClickHouse that returns a stream of rows.

* Uses simple `Row` trait (not `serde`!) for encoding/decoding rows.
* Uses `RowBinaryWithNamesAndTypes` encoding to ensure type safety.
* Supports HTTP (and HTTPS unknown?).
* Provides API for selecting.
* Provides API for inserting.
* TODO: Compression and decompression (LZ4).

## Comparison with the [`clickhouse` crate](https://crates.io/crates/clickhouse)

* Both provide similar performance, with `clickhouse` being a little faster.
* Unlike `clickhouse` which [has an unsound
  API](https://github.com/loyd/clickhouse.rs/issues/24), `streamhouse` does not
  use unsafe (and thus provides a sound API).
* `streamhouse` provides a
  [`futures::Stream`](https://docs.rs/futures/latest/futures/stream/) of rows,
  where `clickhouse` only creates a stream-like
  [`RowCursor`](https://docs.rs/clickhouse/latest/clickhouse/query/struct.RowCursor.html).
* In `clickhouse`, an error in which two columns are swapped in order can give
  incorrect results.  With `streamhouse` this error is caught and reported.
* Because `clickhouse` uses `serde` internally, it is not convenient to create a
  type that can be used with a different serialization (e.g. when converted to
  JSON) than its internal representation in clickhouse.  `streamhouse` uses its
  own traits, so your types can have independent representations as clickhouse
  columns versus other serializations you may wish to use.

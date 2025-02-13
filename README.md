sql-schema
==========

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Build Status](https://github.com/jvatic/sql-schema/actions/workflows/rust.yml/badge.svg?branch=main)](https://github.com/jvatic/sql-schema/actions?query=workflow%3ARust+branch%3Amain)
[![Crates.io Version](https://img.shields.io/crates/v/sql-schema)](https://crates.io/crates/sql-schema)
[![docs.rs](https://img.shields.io/docsrs/sql-schema)](https://docs.rs/sql-schema)

This crate provides tools for generating schema migrations based on edits to a canonical schema file.

## Status

This crate is in a very early stage of development and is missing many features.

## Usage

```sh
# install the cli
cargo install sql-schema

# generate a schema file from existing migrations
sql-schema schema \
    --schema-path ./schema/schema.sql \ # this is the default value
    --migrations-dir ./schema/migrations # this is the default value
# -> writing ./schema/schema.sql

# generate a migration after editing the schema file
sql-schema migration --name my_new_migration \
    --include-down true \ # default is true if any down migration exists
    --schema-path ./schema/schema.sql \ # this is the default value
    --migrations-dir ./schema/migrations # this is the default value
# -> writing schema/migrations/1739486729_my_new_migration.up.sql
# -> writing schema/migrations/1739486729_my_new_migration.down.sql
```

## Licensing

All code in this repository is licensed under the [Apache Software License 2.0](LICENSE.txt).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any additional terms or conditions.

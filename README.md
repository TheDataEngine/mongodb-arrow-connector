# MongoDB Apache Arrow Connector

A Rust library for reading and writing Apache Arrow batches from and to MongoDB.

Licensed under the Apache 2.0 license.

## Motivation

We are curently writing this library due to a need to read MongoDB data into dataframes.

## Features

- [X] Read from a collection to batches
- [X] Write from batches to a collection
- [ ] Infer collection schema
- [ ] Projection predicate push-down
- [ ] Data types
  - [X] Primitive types that MongoDB supports
  - [ ] List types
  - [ ] Nested structs (`bson::Document`)
  - [ ] Arbitrary binary data


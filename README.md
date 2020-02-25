# MongoDB Arrow Connector

A Rust library for Arrow IO from and to MongoDB.

## Motivation

We are curently writing this library due to a need to read MongoDB data into dataframes.

## Features

- [X] Read from a collection to batches
- [ ] Infer collection schema
- [ ] Projection predicate push-down
- [ ] Data types
  - [X] Primitive types that MongoDB supports
  - [ ] List types
  - [ ] Nested structs (`bson::Document`)
  - [ ] Arbitrary binary data


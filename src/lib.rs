//!
//! MongoDB to Apache Arrow Connector
//!
//! This crate allows reading and writing MongoDB data in the Apache Arrow format.
//! Data is read as `RecordBatch`es from a MongoDB database using the aggregation
//! framework.
//! Apache Arrow `RecordBatch`es are written to MongoDB using an insert_many into a collection.

/// MongoDB reader
pub mod reader;
/// MongoDB writer
pub mod writer;

mod utils;

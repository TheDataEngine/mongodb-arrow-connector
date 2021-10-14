use std::iter::FromIterator;
use std::{sync::Arc, time::Duration};

use arrow::{
    array::*,
    datatypes::{DataType, SchemaRef, TimeUnit},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};
use async_stream::stream;
use bson::{doc, Document};
use futures_util::stream::{Stream, StreamExt};
use mongodb::{
    options::{AggregateOptions, ClientOptions, ServerAddress},
    Client,
};

#[derive(Clone, Debug)]
/// Configuration for the MongoDB reader
pub struct ReaderConfig {
    /// The hostname to connect to
    pub hostname: String,
    /// An optional port, defaults to 27017
    pub port: Option<u16>,
    /// The name of the database to read from
    pub database: String,
    /// The name of the collection to read from
    pub collection: String,
    /// Credentials as a struct of key and value
    pub credential: Option<(String, String)>,
    /// Authentication DB if credentials are set
    pub auth_db: Option<String>,
}

/// Database reader
pub struct Reader {
    /// The MongoDB client, with a connection established
    client: Client,
    /// The name of the database to read from
    database: String,
    /// The name of the collection to read from
    collection: String,
    /// The schema of the data to read
    schema: SchemaRef,
    /// The filters to apply
    filters: Vec<Document>,
    /// An internal tracker of the current index that has been read
    current_index: usize,
    /// The preferred batch size per document.
    /// If the documents being read are fairly small, or can fit in memory,
    ///   a larger batch size is more performant as it would result in
    ///   less roundtrips to the database.
    batch_size: usize,
    /// The remaining limit of documents to get after getting each batch
    remaining_limit: usize,
    /// Vector of documents, reused to reduce allocations
    documents: Vec<Document>,
}

impl Reader {
    /// Try to create a new reader
    pub fn try_new(
        config: &ReaderConfig,
        schema: SchemaRef,
        filters: Vec<Document>,
        limit: Option<usize>,
        skip: Option<usize>,
    ) -> Result<Self> {
        let client = get_client(config)?;
        Ok(Self {
            // MongoDB client. The client supports connection pooling, and is suitable for parallel querying
            client,
            // Database name
            database: config.database.to_string(),
            // Collection name
            collection: config.collection.to_string(),
            // The schema of the collection being read
            schema,
            filters,
            // An internal counter to track the number of documents read
            current_index: skip.unwrap_or_default(),
            // The batch size that should be returned from the database
            //
            // If documents are relatively small, or there is ample RAM, a very large batch size should be used
            // to reduce the number of roundtrips to the database
            batch_size: 1024 * 1024,
            remaining_limit: limit.unwrap_or(i32::MAX as usize),
            documents: Vec::with_capacity(1024 * 1024),
        })
    }

    /// Try to estimate the number of rows, to enable partitioning.
    ///
    /// Should time out after a short period to avoid spending too much time.
    pub async fn estimate_records(
        config: &ReaderConfig,
        filters: Vec<Document>,
        max_time_ms: Option<u64>,
    ) -> Result<Option<usize>> {
        let client = get_client(config)?;

        let coll = client
            .database(config.database.as_ref())
            .collection::<Document>(config.collection.as_ref());

        let aggregate_options = AggregateOptions::builder()
            .max_time(Duration::from_millis(max_time_ms.unwrap_or(10000)))
            .build();

        let mut matches = filters;
        matches.push(doc! {"$group": {
            "_id": 1,
            "count": {
                "$sum": 1
            }
        } });

        let cursor = coll.aggregate(matches, Some(aggregate_options)).await;
        if cursor.is_err() {
            return Ok(None);
        }

        let mut cursor = cursor.unwrap();

        let record = cursor.next().await;
        match record {
            Some(result) => {
                let document = result.unwrap();
                let count = document.get_i32("count").unwrap();
                Ok(Some(count as usize))
            }
            None => Ok(None),
        }
    }

    /// Get reader schema
    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    /// Read the next record batch
    pub async fn next_batch(&mut self) -> Result<Option<RecordBatch>> {
        let mut criteria = doc! {};
        let mut project = doc! {};
        let mut matches = vec![];
        for field in self.schema.fields() {
            project.insert(field.name(), bson::Bson::Int32(1));
        }
        criteria.insert("$project", project);
        // add filters
        let mut filters = self.filters.clone();
        if !self.filters.is_empty() {
            matches.append(&mut filters);
        }
        let coll = self
            .client
            .database(self.database.as_ref())
            .collection::<Document>(self.collection.as_ref());

        let aggregate_options = AggregateOptions::builder()
            .batch_size(Some(self.batch_size as u32))
            .build();

        // push projection to matches
        matches.push(criteria);
        matches.push(doc! {"$skip": self.current_index as i32});
        let limit = self.batch_size.min(self.remaining_limit);
        if limit == 0 {
            return Ok(None);
        }
        matches.push(doc! {"$limit": limit as i32});
        self.current_index += self.batch_size;
        self.remaining_limit -= limit;

        let mut cursor = coll
            .aggregate(matches, Some(aggregate_options))
            .await
            .expect("Unable to run aggregation");

        // collect results from cursor into batches
        self.documents.clear();
        for _ in 0..self.batch_size {
            if let Some(Ok(doc)) = cursor.next().await {
                self.documents.push(doc);
            } else {
                break;
            }
        }

        let docs_len = self.documents.len();
        if docs_len == 0 {
            return Ok(None);
        }

        let mut arrays = Vec::with_capacity(self.schema().fields().len());

        for field in self.schema().fields() {
            match field.data_type() {
                DataType::Binary => {
                    let array = BinaryArray::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_binary_generic(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::Boolean => {
                    let array = BooleanArray::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_bool(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::Timestamp(time_unit, _) => {
                    let array = match time_unit {
                        TimeUnit::Second => Arc::new(TimestampSecondArray::from_iter(
                            self.documents.iter().map(|doc| {
                                doc.get_datetime(field.name())
                                    .ok()
                                    .map(|v| v.timestamp_millis() / 1000)
                            }),
                        )) as ArrayRef,
                        TimeUnit::Millisecond => Arc::new(TimestampMillisecondArray::from_iter(
                            self.documents.iter().map(|doc| {
                                doc.get_datetime(field.name())
                                    .ok()
                                    .map(|v| v.timestamp_millis())
                            }),
                        )),
                        TimeUnit::Microsecond => Arc::new(TimestampMicrosecondArray::from_iter(
                            self.documents.iter().map(|doc| {
                                doc.get_datetime(field.name())
                                    .ok()
                                    .map(|v| v.timestamp_millis() * 1000)
                            }),
                        )),
                        TimeUnit::Nanosecond => Arc::new(TimestampNanosecondArray::from_iter(
                            self.documents.iter().map(|doc| {
                                doc.get_datetime(field.name())
                                    .ok()
                                    .map(|v| v.timestamp_millis() * 1_000_000)
                            }),
                        )),
                    };
                    arrays.push(array)
                }
                DataType::Float64 => {
                    let array = Float64Array::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_f64(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::Int32 => {
                    let array = Int32Array::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_i32(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::Int64 => {
                    let array = Int64Array::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_i64(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::Utf8 => {
                    let array = StringArray::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_str(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::LargeUtf8 => {
                    let array = LargeStringArray::from_iter(
                        self.documents
                            .iter()
                            .map(|doc| doc.get_str(field.name()).ok()),
                    );
                    arrays.push(Arc::new(array) as ArrayRef);
                }
                DataType::List(_dtype) => panic!("Creating lists not yet implemented"),
                DataType::Struct(_fields) => panic!("Creating nested structs not yet implemented"),
                t => panic!("Data type {:?} not supported when reading from MongoDB", t),
            }
        }

        Ok(Some(RecordBatch::try_new(self.schema(), arrays)?))
    }

    /// Return the reader as a recordbatch stream
    pub async fn to_stream(mut self) -> impl Stream<Item = RecordBatch> + Send {
        stream! {
            while let Ok(Some(batch)) = self.next_batch().await {
                yield batch
            }
        }
    }
}

fn get_client(config: &ReaderConfig) -> Result<Client> {
    let options = ClientOptions::builder()
        .hosts(vec![ServerAddress::Tcp {
            host: config.hostname.to_string(),
            port: config.port,
        }])
        .build();
    Client::with_options(options).map_err(|e| ArrowError::ExternalError(Box::new(e)))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;
    use std::sync::Arc;

    use arrow::csv;
    use arrow::datatypes::{Field, Schema};

    #[tokio::test]
    async fn test_read_collection() -> Result<()> {
        let fields = vec![
            Field::new("_id", DataType::Utf8, false),
            Field::new("trip_id", DataType::Utf8, false),
            Field::new("trip_status", DataType::Utf8, false),
            Field::new("route_name", DataType::Utf8, false),
            Field::new("route_variant", DataType::Utf8, true),
            Field::new(
                "trip_date",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("trip_time", DataType::Int32, false),
            Field::new("direction", DataType::Utf8, false),
            Field::new("line", DataType::Utf8, true),
            Field::new("stop_id", DataType::Utf8, true),
            Field::new("stop_index", DataType::Int32, false),
            Field::new("scheduled_departure", DataType::Int32, false),
            Field::new("observed_departure", DataType::Int32, true),
            Field::new("stop_relevance", DataType::Utf8, false),
        ];
        let schema = Schema::new(fields);
        let config = ReaderConfig {
            hostname: "localhost".to_string(),
            port: None,
            database: "mycollection".to_string(),
            collection: "delays_".to_string(),
            credential: Some(("user".to_string(), "pass".to_string())),
            auth_db: Some("admin".to_string()),
        };
        let mut reader = Reader::try_new(&config, Arc::new(schema), vec![], None, None)?;

        // write results to CSV as the schema would allow
        let file = File::create("./target/debug/delays.csv").unwrap();
        let mut writer = csv::Writer::new(file);
        while let Ok(Some(batch)) = reader.next_batch().await {
            writer.write(&batch).unwrap();
        }
        Ok(())
    }
}

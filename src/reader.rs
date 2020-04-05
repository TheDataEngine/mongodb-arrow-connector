use std::sync::Arc;

use arrow::{
    array::*,
    datatypes::{DataType, Schema, TimeUnit},
    record_batch::{RecordBatch, RecordBatchReader},
};
use bson::{doc, Bson};
use mongodb::{
    options::{AggregateOptions, ClientOptions, StreamAddress},
    Client,
};
// use mongodb_schema_parser::SchemaParser;

pub struct ReaderConfig<'a> {
    pub hostname: &'a str,
    pub port: Option<u16>,
    // read_preference,
    pub database: &'a str,
    pub collection: &'a str,
}

pub struct Reader {
    client: Client,
    database: String,
    collection: String,
    schema: Schema,
    current_index: usize,
    batch_size: usize,
}

impl Reader {
    /// Try to create a new reader
    pub fn try_new(config: &ReaderConfig, schema: Schema) -> Result<Self, ()> {
        let options = ClientOptions::builder()
            .hosts(vec![StreamAddress {
                hostname: config.hostname.to_string(),
                port: config.port,
            }])
            .build();
        // TODO: support connection with uri_string
        let client = Client::with_options(options).expect("Unable to connect to MongoDB");

        Ok(Self {
            /// MongoDB client. The client supports connection pooling, and is suitable for parallel querying
            client,
            /// Database name
            database: config.database.to_string(),
            /// Collection name
            collection: config.collection.to_string(),
            /// The schema of the collection being read
            schema,
            /// An internal counter to track the number of documents read
            current_index: 0,
            /// The batch size that should be returned from the database
            ///
            /// If documents are relatively small, or there is ample RAM, a very large batch size should be used
            /// to reduce the number of roundtrips to the database
            batch_size: 1024000,
        })
    }

    /// Read the next record batch
    pub fn next(&mut self) -> Result<Option<RecordBatch>, ()> {
        let mut criteria = doc! {};
        let mut project = doc! {};
        for field in self.schema.fields() {
            project.insert(field.name(), bson::Bson::I32(1));
        }
        criteria.insert("$project", project);
        let coll = self
            .client
            .database(self.database.as_ref())
            .collection(self.collection.as_ref());

        let aggregate_options = AggregateOptions::builder()
            .batch_size(Some(self.batch_size as u32))
            .build();

        let mut cursor = coll
            .aggregate(
                vec![criteria, doc! {"$skip": self.current_index as i32}],
                Some(aggregate_options),
            )
            .expect("Unable to run aggregation");

        // collect results from cursor into batches
        let mut docs = vec![];
        for _ in 0..self.batch_size {
            if let Some(Ok(doc)) = cursor.next() {
                docs.push(doc);
            } else {
                break;
            }
        }

        let docs_len = docs.len();
        self.current_index = self.current_index + docs_len;
        if docs_len == 0 {
            return Ok(None);
        }

        let mut builder = StructBuilder::from_schema(self.schema.clone(), self.current_index);

        let field_len = self.schema.fields().len();
        for i in 0..field_len {
            let field = self.schema.field(i);
            match field.data_type() {
                DataType::Binary => {}
                DataType::Boolean => {
                    let field_builder = builder.field_builder::<BooleanBuilder>(i).unwrap();
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get_bool(field.name()) {
                            Ok(val) => field_builder.append_value(val).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Timestamp(time_unit, _) => {
                    let field_builder = match time_unit {
                        TimeUnit::Millisecond => builder
                            .field_builder::<TimestampMillisecondBuilder>(i)
                            .unwrap(),
                        t @ _ => panic!("Timestamp arrays can only be read as milliseconds, found {:?}. \nPlease read as milliseconds then cast to desired resolution.", t)
                    };
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get_utc_datetime(field.name()) {
                            Ok(val) => field_builder.append_value(val.timestamp_millis()).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Float64 => {
                    let field_builder = builder.field_builder::<Float64Builder>(i).unwrap();
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get_f64(field.name()) {
                            Ok(val) => field_builder.append_value(val).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Int32 => {
                    let field_builder = builder.field_builder::<Int32Builder>(i).unwrap();
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get_i32(field.name()) {
                            Ok(val) => field_builder.append_value(val).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Int64 => {
                    let field_builder = builder.field_builder::<Int64Builder>(i).unwrap();
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get_i64(field.name()) {
                            Ok(val) => field_builder.append_value(val).unwrap(),
                            Err(_) => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::Utf8 => {
                    let field_builder = builder.field_builder::<StringBuilder>(i).unwrap();
                    for v in 0..docs_len {
                        let doc: &_ = docs.get(v).unwrap();
                        match doc.get(field.name()) {
                            Some(Bson::ObjectId(oid)) => {
                                field_builder.append_value(oid.to_hex().as_str()).unwrap()
                            }
                            Some(Bson::String(val)) => field_builder.append_value(&val).unwrap(),
                            Some(Bson::Null) => field_builder.append_null().unwrap(),
                            Some(t) => panic!(
                                "Option to cast non-string types to string not yet implemented for {:?}", t
                            ),
                            None => field_builder.append_null().unwrap(),
                        };
                    }
                }
                DataType::List(_dtype) => panic!("Creating lists not yet implemented"),
                DataType::Struct(_fields) => panic!("Creating nested structs not yet implemented"),
                t @ _ => panic!("Data type {:?} not supported when reading from MongoDB", t),
            }
        }
        // append true to all struct records
        for _ in 0..docs_len {
            builder.append(true).unwrap();
        }
        Ok(Some(RecordBatch::from(&builder.finish())))
    }
}

impl RecordBatchReader for Reader {
    fn schema(&mut self) -> Arc<Schema> {
        Arc::new(self.schema.clone())
    }
    fn next_batch(&mut self) -> arrow::error::Result<Option<RecordBatch>> {
        self.next().map_err(|_| {
            arrow::error::ArrowError::IoError("Unable to read next batch from MongoDB".to_string())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs::File;

    use arrow::csv;
    use arrow::datatypes::Field;

    #[test]
    fn test_read_collection() -> Result<(), ()> {
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
            hostname: "localhost",
            port: None,
            database: "transitdb",
            collection: "delays_",
        };
        let mut reader = Reader::try_new(&config, schema)?;

        // write results to CSV as the schema would allow
        let file = File::create("./target/debug/delays.csv").unwrap();
        let mut writer = csv::Writer::new(file);
        while let Ok(Some(batch)) = reader.next() {
            writer.write(&batch).unwrap();
        }
        Ok(())
    }
}

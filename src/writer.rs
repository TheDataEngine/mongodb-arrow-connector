use arrow::{
    array::*,
    compute::cast,
    datatypes::{DataType, Field, Schema, TimeUnit},
    error::{ArrowError, Result},
    record_batch::RecordBatch,
};
use bson::{doc, Document};
use mongodb::{
    options::{ClientOptions, ServerAddress},
    Client,
};

use crate::utils::mongo_to_arrow_error;

/// Configuration for the MongoDB writer
pub struct WriterConfig {
    /// The hostname to connect to
    pub hostname: String,
    /// An optional port, defaults to 27017
    pub port: Option<u16>,
    /// The name of the database to write to
    pub database: String,
    /// The name of the collection to write to
    pub collection: String,
    /// Credentials as a struct of key and value
    pub credential: Option<(String, String)>,
    /// Authentication DB if credentials are set
    pub auth_db: Option<String>,
    /// The write mode, whether an existing collection should
    ///  be appended to or overwritten
    pub write_mode: WriteMode,
    /// Whether compatible types should be coerced, for example
    ///  an Int8 type will be written to an Int32 as BSON doesn't have Int8
    pub coerce_types: bool,
}

/// The mode to write to the collection in
pub enum WriteMode {
    /// Do not drop collection, but append to an existing collection.
    /// If the collection does not exist, a new one is created.
    Append,
    /// Try to drop the collection if it exists.
    /// MongoDB returns an error if a collection that does not exist
    ///  is dropped. We log this to the console, but do not return an error.
    Overwrite,
}

/// Database writer
pub struct Writer {
    /// The MongoDB client, with a connection established
    client: Client,
    /// The name of the database to write to
    database: String,
    /// The name of the collection to write to
    collection: String,
    /// The schema of the data to write
    schema: Schema,
}

impl Writer {
    /// Try to create a new writer, with provided writer options and a schema
    pub async fn try_new(config: WriterConfig, schema: Schema) -> Result<Self> {
        // check if data types can be written
        Writer::check_supported_schema(schema.fields(), config.coerce_types)?;
        let options = ClientOptions::builder()
            .hosts(vec![ServerAddress::Tcp {
                host: config.hostname.to_string(),
                port: config.port,
            }])
            .build();
        // TODO: support connection with uri_string
        let client = Client::with_options(options).expect("Unable to connect to MongoDB");
        if let WriteMode::Overwrite = config.write_mode {
            // we ignore the result here as dropping a non-existent collection returns an error
            let drop = client
                .database(&config.database)
                .collection::<Document>(&config.collection)
                .drop(None)
                .await;
            if drop.is_err() {
                println!("Collection does not exist, and was not dropped");
            }
        }

        Ok(Self {
            client,
            database: config.database.to_string(),
            collection: config.collection.to_string(),
            schema,
        })
    }

    /// Write a batch to the database
    pub async fn write(&self, batch: &RecordBatch) -> Result<()> {
        if batch.schema().as_ref() != &self.schema {
            return Err(ArrowError::SchemaError(
                "Schema of record batch does not match writer".to_string(),
            ));
        }
        // the easiest way could be to create struct to document conversions
        let documents = Documents::from(batch);
        let coll = self
            .client
            .database(self.database.as_str())
            .collection(self.collection.as_str());

        coll.insert_many(documents.0, None)
            .await
            .map(|_| {})
            .map_err(mongo_to_arrow_error)
    }

    /// MongoDB supports a subset of Apache Arrow supported types, check if schema can be written
    fn check_supported_schema(fields: &[Field], coerce_types: bool) -> Result<()> {
        for field in fields {
            let t = field.data_type();
            match t {
                DataType::Int8
                | DataType::Int16
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32
                | DataType::Date32
                | DataType::Date64
                | DataType::UInt64 => {
                    if !coerce_types {
                        return Err(ArrowError::InvalidArgumentError(format!(
                            "Data type {:?} not supported unless it is coerced to another type",
                            t
                        )));
                    }
                }
                DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Float32
                | DataType::Float64
                | DataType::Utf8
                | DataType::LargeUtf8
                | DataType::Timestamp(_, _) => {
                    // data types supported without coercion
                }
                DataType::Float16 => {
                    return Err(ArrowError::InvalidArgumentError(
                        "Float16 arrays not supported".to_string(),
                    ));
                }
                DataType::List(data_type)
                | DataType::LargeList(data_type)
                | DataType::FixedSizeList(data_type, _) => {
                    Writer::check_supported_schema(&[(&**data_type).clone()], coerce_types)?;
                }
                DataType::Struct(fields) => {
                    Writer::check_supported_schema(fields, coerce_types)?;
                }
                DataType::Time32(_)
                | DataType::Time64(_)
                | DataType::Duration(_)
                | DataType::Interval(_)
                | DataType::Binary
                | DataType::LargeBinary
                | DataType::FixedSizeBinary(_) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Data type {:?} not supported",
                        t
                    )));
                }
                DataType::Null => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Data type {:?} not supported",
                        t
                    )));
                }
                DataType::Union(_) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Data type {:?} not supported",
                        t
                    )));
                }
                DataType::Dictionary(_, _) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Data type {:?} not supported",
                        t
                    )));
                }
                DataType::Decimal(_, _) => {
                    return Err(ArrowError::InvalidArgumentError(format!(
                        "Data type {:?} not supported",
                        t
                    )));
                }
            }
        }
        Ok(())
    }
}

/// A private struct that uses a newtype pattern, holds the documents to be written
struct Documents(Vec<bson::Document>);

impl From<&RecordBatch> for Documents {
    fn from(batch: &RecordBatch) -> Self {
        let len = batch.num_rows();
        let mut documents = vec![doc! {}; len];
        batch
            .columns()
            .iter()
            .zip(batch.schema().fields().iter())
            .for_each(|(col, field)| match field.data_type() {
                DataType::Boolean => {
                    let array = col
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::UInt8
                | DataType::UInt16
                | DataType::UInt32 => {
                    let array = cast(col, &DataType::Int32).unwrap();
                    let array = array
                        .as_any()
                        .downcast_ref::<Int32Array>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                DataType::Int64 | DataType::UInt64 => {
                    let array = cast(col, &DataType::Int64).unwrap();
                    let array = array
                        .as_any()
                        .downcast_ref::<Int64Array>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                // DataType::Float16 => {}
                DataType::Float32 => {
                    let array = col
                        .as_any()
                        .downcast_ref::<Float32Array>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                DataType::Float64 => {
                    let array = col
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                DataType::Timestamp(_, _) | DataType::Date32 | DataType::Date64 => {
                    let array =
                        cast(col, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
                    let array = array
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            let value = array.value(i);
                            documents[i].insert(
                                field.name(),
                                bson::Bson::DateTime(bson::DateTime::from_millis(value)),
                            );
                        }
                    }
                }
                // DataType::Time32(_) => {}
                // DataType::Time64(_) => {}
                // DataType::Duration(_) => {}
                // DataType::Interval(_) => {}
                // DataType::Binary => {}
                // DataType::FixedSizeBinary(_) => {}
                DataType::Utf8 => {
                    let array = col
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .expect("Unable to unwrap array");
                    #[allow(clippy::needless_range_loop)]
                    for i in 0..len {
                        if !array.is_null(i) {
                            documents[i].insert(field.name(), array.value(i));
                        }
                    }
                }
                DataType::List(_) => panic!("Write support for lists not yet implemented"),
                DataType::FixedSizeList(_, _) => {
                    panic!("Write support for lists not yet implemented")
                }
                DataType::Struct(_) => panic!("Write support for structs not yet implemented"),
                t => panic!("Encountered unwritable data type {:?}", t),
            });

        Self(documents)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::sync::Arc;

    use arrow::datatypes::Field;

    use crate::reader::*;

    #[tokio::test]
    async fn test_write_collection() -> Result<()> {
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
        let reader_config = ReaderConfig {
            hostname: "localhost".to_string(),
            port: None,
            database: "mycollection".to_string(),
            collection: "delays_".to_string(),
            credential: Some(("user".to_string(), "pass".to_string())),
            auth_db: Some("admin".to_string()),
        };
        let mut reader =
            Reader::try_new(&reader_config, Arc::new(schema.clone()), vec![], None, None)?;
        let writer_config = WriterConfig {
            hostname: "localhost".to_string(),
            port: None,
            database: "mycollection".to_string(),
            collection: "delays_2".to_string(),
            credential: Some(("user".to_string(), "pass".to_string())),
            auth_db: Some("admin".to_string()),
            write_mode: WriteMode::Overwrite,
            coerce_types: true,
        };
        let writer = Writer::try_new(writer_config, schema).await?;

        // read from a collection and write to another
        while let Ok(Some(batch)) = reader.next_batch().await {
            writer.write(&batch).await?
        }
        Ok(())
    }
}

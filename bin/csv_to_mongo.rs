use std::sync::Arc;

use arrow::datatypes::*;
use mongodb_arrow_connector::reader::{Reader as MongoReader, ReaderConfig};

fn main() {
    let config = ReaderConfig {
        hostname: "localhost".to_string(),
        port: Some(27018),
        database: "datafusion".to_string(),
        collection: "nyc_taxi".to_string(),
    };
    let nyc_schema = Arc::new(Schema::new(vec![
        Field::new("VendorID", DataType::Utf8, true),
        Field::new("tpep_pickup_datetime", DataType::Utf8, true),
        Field::new("tpep_dropoff_datetime", DataType::Utf8, true),
        Field::new("passenger_count", DataType::Int32, true),
        Field::new("trip_distance", DataType::Utf8, true),
        Field::new("RatecodeID", DataType::Utf8, true),
        Field::new("store_and_fwd_flag", DataType::Utf8, true),
        Field::new("PULocationID", DataType::Utf8, true),
        Field::new("DOLocationID", DataType::Utf8, true),
        Field::new("payment_type", DataType::Utf8, true),
        Field::new("fare_amount", DataType::Float64, true),
        Field::new("extra", DataType::Float64, true),
        Field::new("mta_tax", DataType::Float64, true),
        Field::new("tip_amount", DataType::Float64, true),
        Field::new("tolls_amount", DataType::Float64, true),
        Field::new("improvement_surcharge", DataType::Float64, true),
        Field::new("total_amount", DataType::Float64, true),
    ]));
    let mut reader = MongoReader::try_new(&config, nyc_schema, vec![], Some(1024 * 1024), None).unwrap();
    while let Ok(Some(batch)) = reader.next_batch() {
        println!("There are {} records in the batch", batch.num_rows());
    }
}
